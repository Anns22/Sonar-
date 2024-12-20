from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.orm import Session
from app.auth.auth_bearer import JWTBearer
from app.booking_app_backend_admin.common.constant import CACE_EXP_TIME, CACHE_CAPACITY
from app.booking_app_backend_admin.events.event_producer import pooling_date_range_event, pooling_event
from app.booking_app_backend_admin.models import models
from app.pooling_management.params import DATA_INSERT_SUCCESSFUL, DATA_UPDATE_SUCCESSFUL, POOL_NOT_FOUND, UNIQUE_POOL_NAME
from app.pooling_management.schemas import PoolCreate, PoolUpdate, ServicePoolsDelete
from database import get_db
from app.pooling_management.crud import *
from sqlalchemy.exc import IntegrityError
from loguru import logger
from fastapi.responses import JSONResponse
from sqlalchemy import and_
from app.booking_app_backend_admin.common.helpers import Helper
from app.booking_app_backend_admin.common.DateRangeChecker import DateRangeChecker
from app.booking_app_backend_admin.common.LruCache import LRUCache

cache = LRUCache(capacity=CACHE_CAPACITY,expiration_time_hours=CACE_EXP_TIME)

pooling_router = APIRouter()


@pooling_router.post("/create_pool",  dependencies=[Depends(JWTBearer())])
def create_pool_with_dates(pool: PoolCreate, db: Session = Depends(get_db), user_data: dict = Depends(JWTBearer()),):

    try:
        if isinstance(user_data, bool):
            user_data = {}
        user_id = user_data.get("user_id")
        subscriber_id = user_data.get("subscriber_id")

        logger.info(f"Creating pool with name: {pool.name}", request_id="app")
        if pool.date_ranges:
            date_ranges = convert_pool_date_ranges(pool.date_ranges)
            checker = DateRangeChecker(date_ranges)
            overlaps = checker.find_overlapping_ranges()
            if overlaps:
                for overlap in overlaps:
                    return JSONResponse(status_code=400, content={"success": False, "message": overlap, "translation_key": "POOL_DATES_OVERLAPS"})
            gaps = checker.check_date_gaps()
            if gaps:
                for gap in gaps:
                    return JSONResponse(status_code=400, content={"success": False, "message": gap, "translation_key": "POOL_DATE_GAPS"})
        db_pool = create_pool(
            db=db, pool=pool, user_id=user_id, subscriber_id=subscriber_id)

        if pool.date_ranges:
            db_date_ranges = create_pool_date_ranges(
                db=db, pool_id=db_pool.id, date_ranges=pool.date_ranges, user_id=user_id, subscriber_id=subscriber_id)

        subscriber_details = Helper.get_subscriber_details_from_cache(db, subscriber_id)
        if subscriber_details.get("success") is False:
            return subscriber_details
        else:
            subscriber_details = subscriber_details["data"]
            
        db_date_ranges_json = {'data': db_date_ranges}

        pooling_event("create", db_pool.to_dict(), subscriber_details)
        pooling_date_range_event(
            "create", db_date_ranges_json, subscriber_details)

        return JSONResponse(status_code=200, content={"success": True, "message": DATA_INSERT_SUCCESSFUL, "translation_key": "DATA_INSERT_SUCCESSFUL"})

    except IntegrityError:
        db.rollback()
        return JSONResponse(status_code=400, content={"success": False, "message": UNIQUE_POOL_NAME, "translation_key": "UNIQUE_POOL_NAME"})
    except Exception as e:
        db.rollback()
        logger.exception(f"Error in creating pool: {str(e)}", request_id="app")
        return JSONResponse(status_code=400, content={"success": False, "message": str(e)})


@pooling_router.put("/update_pool",  dependencies=[Depends(JWTBearer())])
def edit_pool(pool_update: PoolUpdate, db: Session = Depends(get_db), user_data: dict = Depends(JWTBearer())):

    try:
        if isinstance(user_data, bool):
            user_data = {}
        user_id = user_data.get("user_id")
        subscriber_id = user_data.get("subscriber_id")
        check_date_ranges = pool_update.check_date_ranges
        if pool_update.date_ranges:
            date_ranges = convert_pool_date_ranges(pool_update.date_ranges)
            checker = DateRangeChecker(date_ranges)
            overlaps = checker.find_overlapping_ranges()
            if overlaps:
                for overlap in overlaps:
                    return JSONResponse(status_code=400, content={"success": False, "message": overlap, "translation_key": "POOL_DATES_OVERLAPS"})
            gaps = checker.check_date_gaps()
            if gaps:
                for gap in gaps:
                    return JSONResponse(status_code=400, content={"success": False, "message": gap, "translation_key": "POOL_DATE_GAPS"})

        db_pool = update_pool(db, pool_update.id, pool_update, user_id)

        if not db_pool:
            return JSONResponse(status_code=404, content={
                "success": False, "message": POOL_NOT_FOUND, "translation_key": "POOL_NOT_FOUND"})

        if pool_update.date_ranges:
            updated_date_range = update_pool_date_ranges(
                db, pool_update.id, pool_update.date_ranges, user_id, subscriber_id, check_date_ranges)
            if type(updated_date_range) == dict and updated_date_range.get("success") is False:
                return updated_date_range

        deleted_ids, updated_data = updated_date_range

        subscriber_details = Helper.get_subscriber_details_from_cache(db, subscriber_id)
        if subscriber_details.get("success") is False:
            return subscriber_details
        else:
            subscriber_details = subscriber_details["data"]

        deleted_ids_json = {'ids': deleted_ids}
        updated_data_json = {'data': updated_data}

        pooling_event("update", db_pool.to_dict(), subscriber_details)
        pooling_date_range_event(
            "delete", deleted_ids_json, subscriber_details)
        pooling_date_range_event(
            "create", updated_data_json, subscriber_details)

        return JSONResponse(status_code=200, content={"success": True, "message": DATA_UPDATE_SUCCESSFUL, "translation_key": "DATA_UPDATE_SUCCESSFUL"})

    except IntegrityError:
        db.rollback()
        return JSONResponse(status_code=400, content={"success": False, "message": UNIQUE_POOL_NAME, "translation_key": "UNIQUE_POOL_NAME"})
    except Exception as e:
        db.rollback()
        logger.exception(f"Error in updating pool: {str(e)}", request_id="app")
        return JSONResponse(status_code=400, content={"success": False, "message": str(e)})


@pooling_router.get(
    "/service-pools/list",
    summary="list_service_pools",
    dependencies=[Depends(JWTBearer())],
)
def list_service_pools(
    response: Response,
    user_data: dict = Depends(JWTBearer()),
    db: Session = Depends(get_db),
    page: int = Query(1),
    page_size: int = Query(None),
    id: int = None,
):
    """
    Retrieve a paginated list of service pools.
    This endpoint allows the user to retrieve a list of service pools based on optional filters.
    
    Args:
        response (Response): The response object to set status codes.
        user_data (dict, optional): Data about the authenticated user, extracted from JWT. Defaults to an empty dict if not authenticated.
        db (Session): Database session dependency.
        page (int, optional): The page number to fetch. Defaults to 1.
        page_size (int, optional): Number of records per page. Defaults to 50.
        id (int, optional): Optional filter to retrieve service pools based on an ID. Defaults to None.
    Returns:
        dict: Contains success status, list of service pools, and the total number of records.
    """

    if isinstance(user_data, bool):
        user_data = {}

    user_id = user_data.get("user_id")
    subscriber_id = user_data.get("subscriber_id")
    page = page or 1
    page_size = page_size
    if page_size is None:
        org_settings = Helper.get_org_settings_from_cache(db=db, subscriber_id=subscriber_id, cache=cache)
        if org_settings is not None and "errors" in org_settings:
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            return org_settings
        page_size = org_settings.get("api_limit")

    params = {
        "user_id": user_id,
        "subscriber_id": subscriber_id,
        "pool_id": id,
        "page_size": page_size,
        "page": page,
    }

    service_pools_list = pools_list(db, params, response)

    if isinstance(service_pools_list, dict) and not service_pools_list.get("success"):
        return service_pools_list 

    if not service_pools_list:
        return {
            "success": True,
            "data": [],
            "message": "No Records Found",
            "translation_key": "NO_RECORDS_FOUND",
            "recordsTotal": 0,
        }
    return {
        "success": True,
        "data": service_pools_list,
        "recordsTotal": len(service_pools_list),
    }

@pooling_router.delete(
    "/service-pools/delete",
    summary="delete_service_pools",
    dependencies=[Depends(JWTBearer())],
)
def delete_service_pool(
    to_delete: ServicePoolsDelete,
    response: Response,
    user_data: dict = Depends(JWTBearer()),
    db: Session = Depends(get_db),
):
    """
    Delete a service pool based on the provided data.
    This endpoint deletes a specific service pool record based on the user's authentication and provided data.
    Args:
        to_delete (ServicePoolsDelete): The service pool data that needs to be deleted.
        response (Response): The response object to set status codes.
        user_data (dict, optional): Data about the authenticated user, extracted from JWT. Defaults to an empty dict if not authenticated.
        db (Session): Database session dependency.
    Returns:
        dict: Contains success status and response message after attempting to delete the service pool.
    """
    if isinstance(user_data, bool):
        user_data = {}

    user_id = user_data.get("user_id")
    subscriber_id = user_data.get("subscriber_id")

    params = {
        "subscriber_id": subscriber_id,
        "user_id": user_id,
        "data": to_delete,
    }

    delete_pool = service_pools_delete(
        db,
        params,
        response,
    )
    return delete_pool