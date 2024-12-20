

from datetime import date, datetime
import json
from typing import Dict, List, Optional, Type
import grpc
from sqlalchemy.orm import Session
from app.booking_app_backend_admin.common.params import DELETED_DEFAULT, RECORD_STATUS_DEFAULT, DELETE_SUCCESS_MESSAGE, ERROR_ON_DELETE
from app.booking_app_backend_admin.models import models
from app.pooling_management.models import Pool, PoolDateRange, PoolDateRangeHistory
from app.pooling_management.schemas import PoolCreate, PoolDateRangeCreate
from sqlalchemy import and_, insert
from app.pooling_management.params import *
from loguru import logger
from fastapi import Response, status
from app.booking_app_backend_admin.events.event_producer import pooling_date_range_event, pooling_event
from app.protos import bookingapp_pb2, bookingapp_pb2_grpc

def create_pool(db: Session, pool: PoolCreate, user_id: int, subscriber_id: int):
    '''
    Create a new pool in the database.

    Parameters:
    - db: Session object for database interaction.
    - pool: PoolCreate object containing pool details.
    - user_id: Integer representing the ID of the user creating the pool.
    - subscriber_id: Integer representing the ID of the subscriber associated with the pool.

    Returns:
    - The newly created Pool object.
    '''
    db_pool = Pool(name=pool.name, remarks=pool.remarks,
                   created_by_id=user_id, subscriber_id=subscriber_id)
    db.add(db_pool)
    db.commit()
    db.refresh(db_pool)
    return db_pool


def get_subscriber_details(db: Session, subscriber_id: int) -> models.Subscriber:

    subscriber_details = db.query(models.Subscriber).filter(and_(models.Subscriber.id == subscriber_id,
                                                                 models.Subscriber.deleted == DELETED_DEFAULT, 
                                                                 models.Subscriber.record_status == RECORD_STATUS_DEFAULT)).first()
    
    return subscriber_details


def create_pool_date_ranges(db: Session, pool_id: int, date_ranges: List[PoolDateRangeCreate], user_id: int, subscriber_id: int):
    '''
    Create pool date ranges in the database and log the changes in the history table.

    Parameters:
    - db: Session object for the database connection
    - pool_id: ID of the pool for which date ranges are being created
    - date_ranges: List of PoolDateRangeCreate objects containing start date, end date, and capacity
    - user_id: ID of the user performing the operation
    - subscriber_id: ID of the subscriber associated with the pool

    Returns:
    - List of dictionaries representing the created date ranges
    '''
    date_ranges_dict = []

    for range_data in date_ranges:
        db_date_range = PoolDateRange(
            pool_id=pool_id,
            start_date=range_data.start_date,
            end_date=range_data.end_date,
            capacity=range_data.capacity,
            created_by_id=user_id,
            subscriber_id=subscriber_id,
        )
        db.add(db_date_range)
        db.commit()
        db.refresh(db_date_range)

        date_ranges_dict.append(db_date_range.to_dict())

        log_pool_date_range_history(
            db=db,
            pool_id=pool_id,
            pool_date_range_id=db_date_range.id,
            action_type="INSERT",
            old_start_date=None,  # No old data for an insert
            old_end_date=None,
            old_capacity=None,
            user_id=user_id,
            subscriber_id=subscriber_id
        )

    return date_ranges_dict


def update_pool(db: Session, pool_id: int, pool_update: PoolCreate, user_id: int):
    """
    Update a pool in the database.

    Parameters:
    - db: Session: The database session.
    - pool_id: int: The ID of the pool to update.
    - pool_update: PoolCreate: The updated pool information.
    - user_id: int: The ID of the user performing the update.

    Returns:
    - Pool: The updated pool object if successful, None otherwise.
    """
    db_pool = db.query(Pool).filter(Pool.id == pool_id,
                                    Pool.deleted == 0, Pool.record_status == 1).first()
    if not db_pool:
        return None

    if pool_update.name is not None:
        db_pool.name = pool_update.name
        db_pool.updated_by_id = user_id
    if pool_update.remarks is not None:
        db_pool.remarks = pool_update.remarks

    db.commit()
    db.refresh(db_pool)

    return db_pool


def update_pool_date_ranges(db: Session, pool_id: int, date_ranges: List[PoolDateRangeCreate], user_id: int, subscriber_id: int, check_date_ranges=False):
    """
    Update pool date ranges by deleting existing ones, inserting new ones, and logging the changes in history.

    Parameters:
    - db: Session object for the database connection
    - pool_id: ID of the pool to update date ranges for
    - date_ranges: List of PoolDateRangeCreate objects representing the new date ranges to update
    - user_id: ID of the user performing the update
    - subscriber_id: ID of the subscriber associated with the pool

    Returns:
    - deleted_ids: List of IDs of the deleted date ranges
    - updated_data: List of dictionaries representing the updated date ranges
    """
    # Fetch existing date ranges before deletion
    deleted_ids = []
    updated_data = []
    existing_ranges = db.query(PoolDateRange).filter(
        PoolDateRange.pool_id == pool_id,
        PoolDateRange.deleted == 0,
        PoolDateRange.record_status == 1
    ).all()

    # Log the old data in history table before deletion
    if not check_date_ranges:
        check_date_capacity = validate_service_pool_changes(existing_ranges, date_ranges, pool_id, subscriber_id)
        if check_date_capacity['success'] == False:
            return check_date_capacity


    for old_range in existing_ranges:
        deleted_ids.append(old_range.id)
        log_pool_date_range_history(
            db=db,
            pool_id=pool_id,
            pool_date_range_id=old_range.id,
            action_type="DELETE",
            old_start_date=old_range.start_date,
            old_end_date=old_range.end_date,
            old_capacity=old_range.capacity,
            user_id=user_id,
            subscriber_id=subscriber_id
        )

    # Delete old ranges
    db.query(PoolDateRange).filter(
        PoolDateRange.pool_id == pool_id,
        PoolDateRange.deleted == 0,
        PoolDateRange.record_status == 1
    ).delete()

    # Insert new ranges and log them into history
    for date_range in date_ranges:
        new_date_range = PoolDateRange(
            pool_id=pool_id,
            start_date=date_range.start_date,
            end_date=date_range.end_date,
            capacity=date_range.capacity,
            created_by_id=user_id,
            updated_by_id=user_id,
            subscriber_id=subscriber_id
        )
        db.add(new_date_range)
        db.commit()
        db.refresh(new_date_range)
        updated_data.append(new_date_range.to_dict())

        # Log the new date range into the history table
        log_pool_date_range_history(
            db=db,
            pool_id=pool_id,
            pool_date_range_id=new_date_range.id,
            action_type="INSERT",
            old_start_date=None,  # No old data for insert
            old_end_date=None,
            old_capacity=None,
            user_id=user_id,
            subscriber_id=subscriber_id
        )

    db.commit()
    return deleted_ids, updated_data

def check_booking_available(pool_id: int, start_date: date, end_date: date, capacity: int, subscriber_id: int, capacity_check: bool = False):
    """
    This Python function checks the availability of a booking for a pool based on specified parameters.
    
    :param pool_id: An integer representing the ID of the pool for which availability needs to be
    checked
    :type pool_id: int
    :param start_date: The `check_booking_available` function seems to be designed to check the
    availability of bookings for a pool based on certain parameters. The parameters are as follows:
    :type start_date: date
    :param end_date: The `check_booking_available` function seems to be designed to check the
    availability of bookings for a pool based on the provided parameters. Here is a breakdown of the
    parameters:
    :type end_date: date
    :param capacity: The `capacity` parameter in the `check_booking_available` function represents the
    number of people that the booking is for. It is used to check if there is enough capacity available
    at the specified pool for the booking
    :type capacity: int
    :param subscriber_id: subscriber_id is the unique identifier for the subscriber who is making the
    booking
    :type subscriber_id: int
    :param capacity_check: The `capacity_check` parameter is a boolean flag that indicates whether the
    function should perform a capacity check when checking for booking availability. If `capacity_check`
    is set to `True`, the function will consider the capacity parameter when determining if the booking
    is available for the specified pool and dates. If `, defaults to False
    :type capacity_check: bool (optional)
    """
    
    with grpc.insecure_channel('booking:50051') as channel:
        stub = bookingapp_pb2_grpc.GetBookingDetailsStub(channel)
        data = {
            "pool_id": pool_id,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "capacity": capacity,
            "subscriber_id": subscriber_id,
            "capacity_check" : capacity_check,
        }
        request_proto = bookingapp_pb2.RequestBooking(
            data = json.dumps(data)
        )
        response = stub.CheckBookings(request_proto)
        return json.loads(response.result)
def log_pool_date_range_history(db: Session, pool_id: int, pool_date_range_id: int, action_type: str,
                                old_start_date: date = None, old_end_date: date = None, old_capacity: int = None,
                                user_id: int = None, subscriber_id: int = None):
    """
    Log changes in the pool date ranges to the pool_date_range_history table.
    """
    history_entry = PoolDateRangeHistory(
        pool_id=pool_id,
        pool_date_range_id=pool_date_range_id,
        action_type=action_type,
        old_start_date=old_start_date,
        old_end_date=old_end_date,
        old_capacity=old_capacity,
        created_by_id=user_id,
        subscriber_id=subscriber_id,
    )

    db.add(history_entry)
    db.commit()

def convert_pool_date_ranges(pool_date_ranges)-> List[Dict[str, str]]:
    """
    Converts a list of DateRange objects to a list of dictionaries with string date representations.

    Args:
        pool_date_ranges (List[DateRange]): A list of DateRange objects.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing 'start_date' and 'end_date' as strings.
    """
    converted_ranges = []

    for range_obj in pool_date_ranges:
        # Convert the start and end dates to strings (handling None for end_date)
        start_str = range_obj.start_date.strftime('%Y-%m-%d')
        end_str = range_obj.end_date.strftime('%Y-%m-%d') if range_obj.end_date else '9999-12-31'  # or any appropriate default
        
        converted_ranges.append({
            'start_date': start_str,
            'end_date': end_str,
        })

    return converted_ranges


def pools_list(db: Session, params: dict, response: Response) -> List[dict]:
    """
    The function `pools_list` retrieves a list of pools based on specified parameters from a database
    and handles exceptions by logging errors and returning an error response.
    :return: The `pools_list` function returns a list of dictionaries containing pool information based
    on the provided parameters. If an error occurs during the execution of the function, it returns a
    dictionary with error information and an HTTP 500 status code.
    """
    pools_list = []
    subscriber_id: str = params.get("subscriber_id")
    pool_id: Optional[str] = params.get("pool_id", None)
    page_size: Optional[int] = params.get("page_size", None)
    page: int = params.get("page")

    try:
        pool_query = (
            db.query(Pool)
            .filter(
                Pool.deleted == DELETE_STATUS,
                Pool.record_status <= LIST_RECORD_STATUS,
                Pool.subscriber_id == subscriber_id
            )
            .order_by(Pool.id.desc())
        )

        if pool_id:
            pool_query = pool_query.filter(Pool.id == pool_id)
        if page_size:
            pool_query = pool_query.offset((page - 1) * page_size).limit(page_size)

        pools = pool_query.all()

        if pools:
            pool_ids = [pool.id for pool in pools]

            date_range_query = (
                db.query(PoolDateRange)
                .filter(PoolDateRange.pool_id.in_(pool_ids))
                .order_by(PoolDateRange.pool_id.asc(), PoolDateRange.start_date.asc())
            )

            date_ranges = date_range_query.all()

            pool_date_map = {}
            for date_range in date_ranges:
                if date_range.pool_id not in pool_date_map:
                    pool_date_map[date_range.pool_id] = []

                pool_date_map[date_range.pool_id].append({
                    "start_date": date_range.start_date,
                    "end_date": date_range.end_date,
                    "capacity": date_range.capacity
                })

            for pool in pools:
                pool_dict = {
                    "id": pool.id,
                    "name": pool.name,
                    "remarks": pool.remarks,
                    "date_ranges": pool_date_map.get(pool.id, [])
                }
                pools_list.append(pool_dict)

        return pools_list

    except Exception as e:
        logger.error(
            "Error in service_pools_list: {}",
            {str(e)},
            request_id="app",
        )
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {
            "success": False,
            "api_error": {str(e)},
            "data": [],
            "translation_key": "ERROR_ON_SERVICE_POOLS_LIST",
        }
    finally:
        db.close()  

def check_linked_services(db: Session, pool_id: int, subscriber_id: int, pool_name: str):
    """
    This function checks for linked services associated with a specific pool and subscriber, and
    generates a message based on the number of linked services found.
    """
    linked_services = db.query(models.Service).filter(
        models.Service.pool_id == pool_id,
        models.Service.subscriber_id == subscriber_id,
        models.Service.deleted == DELETE_STATUS,
        models.Service.record_status <= LIST_RECORD_STATUS,
        models.Service.capacity_type_id == CHECK_CAPACITY_TYPE_ID,
    ).all()

    if linked_services:
        service_names = [service.name for service in linked_services]
        if len(service_names) == 1:
            services_list = service_names[0]
            message = UNABLE_TO_DELETE_POOL.replace("<pool-name>", pool_name).replace("<services-list>", services_list + " service").replace("<the>", THAT)
        else:
            services_list = ", ".join(service_names[:-1]) + " and " + service_names[-1]
            message = UNABLE_TO_DELETE_POOL.replace("<pool-name>", pool_name).replace("<services-list>", services_list + " services").replace("<the>", EACH)
        return (True, message)

    return (False, None)

def bulk_insert_history(
    db: Session,
    model: Type,  # The dynamic history model (e.g., PoolDateRangeHistory)
    data: List[dict],  # List of dictionaries containing data to insert
    response: Response,  # The response object to set the status code
) -> None:
    """
    Performs a bulk insert of history entries into the specified model.

    :param db: The SQLAlchemy database session.
    :param model: The history model (e.g., PoolDateRangeHistory) to insert records into.
    :param data: A list of dictionaries, each containing the columns and values to be inserted.
    """
    if not data:
        return None # No data to insert
    
    try:
        db.execute(insert(model), data)
        db.commit()
    except Exception as e:
        logger.error(
            "Error in inserting data in history table: {}",
            {str(e)},
            request_id="app",
        )
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {
            "success": False,
            "message": ERROR_ON_HISTORY_TABLE_INSERT,
            "api_error": {str(e)},
            "translation_key": "ERROR_ON_HISTORY_TABLE_INSERT",
        }


def service_pools_delete(db: Session, params: dict, response):
    """
    The function `service_pools_delete` deletes service pools based on certain conditions and handles
    error cases: The function `service_pools_delete` returns a dictionary with keys "success", "message",
    and "translation_key" based on the outcome of the deletion operation. The specific content of the
    dictionary varies depending on the conditions met during the deletion process.
    """
    try:
        subscriber_id = params.get("subscriber_id")
        user_id = params.get("user_id")
        request_data = params.get("data")
        service_pools = (
            db.query(Pool)
            .filter(
                Pool.id.in_(request_data.ids),
                Pool.subscriber_id == subscriber_id,
                Pool.deleted == DELETE_STATUS,
                Pool.record_status <= LIST_RECORD_STATUS,
            )
            .all()
        )
        if not service_pools:
            response.status_code = status.HTTP_404_NOT_FOUND
            return {
                "success": False,
                "message": NO_SERVICE_POOLS_TO_DELETE,
                "translation_key": "NO_SERVICE_POOLS_TO_DELETE",
            }
        for id in request_data.ids:
            service_pool = next((pool for pool in service_pools if pool.id == id), None)
            if not service_pool:
                logger.error(
                    "No service_pool found for id: {}",
                    {id},
                    request_id="app",
                )
                continue

            has_linked_services, message = check_linked_services(db, id, subscriber_id, service_pool.name)
            if has_linked_services:
                response.status_code = status.HTTP_400_BAD_REQUEST
                return {
                    "success": False,
                    "message": message,
                    "translation_key": "SERVICE_POOL_IN_USE",
                }
            service_pool.deleted = DELETED
            service_pool.updated_by_id = user_id

            pool_capacities = db.query(PoolDateRange).filter(PoolDateRange.pool_id == id, PoolDateRange.subscriber_id == subscriber_id).order_by(PoolDateRange.id.asc()).all()

            if pool_capacities:
                history_entries = [
                    {
                        "pool_id": capacity.pool_id,
                        "pool_date_range_id": capacity.id,
                        "action_type": DELETE, 
                        "old_start_date": capacity.start_date,
                        "old_end_date": capacity.end_date,
                        "old_capacity": capacity.capacity,
                        "created_by_id": user_id,
                        "subscriber_id": subscriber_id,
                        "updated_by_id": user_id,
                    }
                    for capacity in pool_capacities
                ]
                bulk_insert_history(db, PoolDateRangeHistory, history_entries, response)
                for capacity in pool_capacities:
                    db.delete(capacity)
            db.commit()
            subscriber_details = get_subscriber_details(db, subscriber_id)
            pool_params = {
                "pool_id": id,
                "updated_by_id": user_id,
                "subscriber_id": subscriber_id,
            }
            pooling_event("delete", pool_params, subscriber_details)
        return {
            "success": True,
            "message": DELETE_SUCCESS_MESSAGE,
            "translation_key": "DELETE_SUCCESS_MESSAGE",
        }
    except Exception as e:
        logger.error(
            "Error in deleting service_pools: {}",
            {str(e)},
            request_id="app",
        )
        db.rollback()
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {
            "success": False,
            "message": ERROR_ON_DELETE,
            "api_error": {str(e)},
            "translation_key": "ERROR_ON_DELETE",
        }    

def validate_service_pool_changes(existing_ranges, date_ranges, pool_id, subscriber_id):
    """
    Validates service pool changes for date ranges and capacity modifications.
    Handles all scenarios including date range changes, capacity reduction, and combined changes.
    """
    
    for old_range, date_range in zip(existing_ranges, date_ranges):
        start1, end1 = old_range.start_date, old_range.end_date
        start2, end2 = date_range.start_date, date_range.end_date
        old_capacity, new_capacity = old_range.capacity, date_range.capacity
        
        # Basic date validations
        current_date = datetime.now().date()
        if (end2 and end2 < current_date):
            return {"success": False, "dialogue": False, "message": PAST_DATE_ERROR}
            
        if end2 and start2 > end2:
            return {"success": False, "dialogue": False, "message": DATE_VALIDATION}

        # Determine if there's a capacity reduction
        is_capacity_reduced = old_capacity > new_capacity

        # First check date range changes
        date_range_changed = False
        excluded_date_ranges = []

        # Scenario 1: Start date changed
        if start2 != start1:
            date_range_changed = True
            if start2 < start1:  # Start Earlier
                excluded_date_ranges.append((start2, start1))
            else:  # Start Later
                excluded_date_ranges.append((start1, start2))

        # Scenario 2: End date changed
        if end1 and end2 and end2 != end1:
            date_range_changed = True
            if end2 < end1:  # End Earlier
                excluded_date_ranges.append((end2, end1))
            else:  # End Later
                excluded_date_ranges.append((end1, end2))

        # If date ranges changed, check for bookings in excluded periods
        if date_range_changed:
            for excluded_start, excluded_end in excluded_date_ranges:
                booking_check = check_booking_available(
                    pool_id=pool_id,
                    start_date=excluded_start,
                    end_date=excluded_end,
                    capacity=old_capacity,
                    subscriber_id=subscriber_id
                )

                if booking_check.get('is_booking_exists', False):
                    # Scenario 4: Combined date range and capacity changes
                    if is_capacity_reduced:
                        capacity_check = check_booking_available(
                            pool_id=pool_id,
                            start_date=excluded_start,
                            end_date=excluded_end,
                            capacity=new_capacity,
                            subscriber_id=subscriber_id,
                            capacity_check=True
                        )
                        
                        if capacity_check.get('is_capacity_check', False):
                            return {
                                "success": False,
                                "dialogue": True,
                                "message": REDUCED_DATE_CAPACITY_MESSAGE
                            }
                    
                    # Regular date range change
                    return {
                        "success": False,
                        "dialogue": True,
                        "message": REDUCE_DATE_MESSAGE
                    }
        
        # If no date range issues, then check standalone capacity reduction (Scenario 3)
        if is_capacity_reduced:
            # Check entire date range for capacity issues
            capacity_check = check_booking_available(
                pool_id=pool_id,
                start_date=start1,  # Use original date range for pure capacity check
                end_date=end1,
                capacity=new_capacity,
                subscriber_id=subscriber_id,
                capacity_check=True
            )
            
            if capacity_check.get('is_booking_exists', False) and capacity_check.get('is_capacity_check', False):
                return {
                    "success": False,
                    "dialogue": True,
                    "message": REDUCE_CAPACITY_MESSSAGE
                }

    # If all validations pass
    return {"success": True, "dialogue": False}

