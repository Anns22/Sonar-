from types import SimpleNamespace
from fastapi import Response, status
from sqlalchemy.orm import Session
from app.CalendarManagement.CalendarManagementComponent import (
    CalendarManagementComponent,
)
from app.CalendarManagement.params import (
    NO_RULES_FOUND,
    NOTAVAILABLE,
    STARTDATEGREATERTHANENDDATE,
    INVALIDDATEFORMAT,
)
from app.StaffSchedule.helper import Helper
from datetime import datetime, date
from typing import Union, Dict, Any, Set


class NotAvailableRuleComponent:
    def get_not_available_rule_dates(
        db: Session, request: Dict[str, Any], response: Response, subscriber_id: int
    ) -> Dict[str, Any]:
        """
        This function retrieves not available rule dates based on specific criteria for a subscriber.

        :param db: The `db` parameter is of type `Session` and is used to interact with the database. It
        is typically used to execute queries and transactions within the database session
        :type db: Session
        :param request: The `request` parameter in the `get_not_available_rule_dates` function seems to
        contain the following attributes:
        :type request: dict
        :param response: The `response` parameter in the `get_not_available_rule_dates` function is used
        to send back the response to the caller of the function. It is of type `Response` and is used to
        provide information about the outcome of the function execution, such as success or failure,
        along with any relevant
        :type response: Response
        :param subscriber_id: Subscriber ID is an identifier for a subscriber in the system. It is used
        to distinguish different subscribers who are using the system or service
        :type subscriber_id: int
        :return: The function `get_not_available_rule_dates` returns a dictionary with the following
        possible keys:
        - "success": A boolean indicating whether the operation was successful or not.
        - "not_available_dates": A set containing the unavailable dates based on the provided
        parameters.
        """
        try:
            unavailable_dates :Set[str] = set()
            service_id = request.service_id
            slot_id = request.slot_id
            start_date = request.start_date
            end_date = request.end_date
            user_id = request.customer_id
            user_tag_ids = CalendarManagementComponent.get_user_tag_ids(
                db,
                user_id,
                subscriber_id,
            )
            meta_params: Dict[str, Any] = {}
            meta_params["subscriber_id"] = subscriber_id
            meta_params["service_id"] = service_id
            meta_params["slot_id"] = slot_id
            meta_params["beneficiary_ids"] = []
            user_service_rules = CalendarManagementComponent.get_user_service_rules(
                db,
                service_id,
                slot_id,
                user_tag_ids,
                None,
                None,
                response,
                meta_params,
            )
            if user_service_rules is not None and "errors" in user_service_rules:
                return user_service_rules
            else:
                filtered_rules = user_service_rules["filtered_rules"]
                filtered_rules = [
                    item
                    for item in user_service_rules["filtered_rules"]
                    if item.pricing_type == NOTAVAILABLE
                ]
                if filtered_rules:
                    for rule in filtered_rules:
                        rule_start_date = None
                        rule_end_date = None
                        adjust_rule_dates = adjust_dates(
                            rule.start_date, rule.end_date, start_date, end_date
                        )
                        if adjust_rule_dates["success"] is False:
                            response.status_code = status.HTTP_400_BAD_REQUEST
                            return adjust_rule_dates
                        else:
                            rule_start_date = str(adjust_rule_dates["start_date"])
                            rule_end_date = str(adjust_rule_dates["end_date"])
                        rule_data = {
                            "repeat_type": rule.repeat_type,
                            "repeat_details": rule.repeat_details,
                            "start_date": rule_start_date,
                            "end_date": rule_end_date,
                        }
                        unavailable_dates.update(
                            CalendarManagementComponent.getUnAvailableDates(
                                rule_data=rule_data
                            )
                        )

                    return {
                        "success": True,
                        "not_available_dates": unavailable_dates,
                    }
                else:
                    response.status_code = status.HTTP_400_BAD_REQUEST
                    return Helper.error_response(
                        response,
                        "NO_RULES_FOUND",
                        NO_RULES_FOUND,
                        status.HTTP_400_BAD_REQUEST,
                    )

        except Exception as e:
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            return Helper.error_response(
                response,
                "INTERNAL_SERVER_ERROR",
                str(e),
                status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        finally:
            if db:
                db.close()


@staticmethod
def adjust_dates(
    rule_start_date: Union[str, date],
    rule_end_date: Union[str, date],
    request_start_date: Union[str, date],
    request_end_date: Union[str, date],
) -> Dict[str, Union[bool, date, str]]:
    """
    Adjusts the start and end dates based on provided rule dates and request dates.
    Parameters:
        rule_start_date (Union[str, date]): The start date of the rule.
        rule_end_date (Union[str, date]): The end date of the rule.
        request_start_date (Union[str, date]): The requested start date.
        request_end_date (Union[str, date]): The requested end date.
    Returns:
        Dict[str, Union[bool, date, str]]: A dictionary containing success status,
        adjusted start and end dates if successful, or error information if not.
    """
    try:
        # Convert all dates from string to datetime.date objects if they are strings
        rule_start_date = (
            datetime.strptime(rule_start_date, "%Y-%m-%d").date()
            if isinstance(rule_start_date, str)
            else rule_start_date
        )
        rule_end_date = (
            datetime.strptime(rule_end_date, "%Y-%m-%d").date()
            if isinstance(rule_end_date, str)
            else rule_end_date
        )
        request_start_date = (
            datetime.strptime(request_start_date, "%Y-%m-%d").date()
            if isinstance(request_start_date, str)
            else request_start_date
        )
        request_end_date = (
            datetime.strptime(request_end_date, "%Y-%m-%d").date()
            if isinstance(request_end_date, str)
            else request_end_date
        )

        if request_start_date > request_end_date:
            return {
                "success": False,
                "message": STARTDATEGREATERTHANENDDATE,
                "translation_key": "START_DATE_GREATER_THAN_END_DATE"
            }
        # Adjust the start date: it should be the max between rule_start_date and request_start_date
        adjusted_start_date = max(request_start_date, rule_start_date)

        # Adjust the end date: it should be the min between rule_end_date and request_end_date
        adjusted_end_date = min(request_end_date, rule_end_date)
        return {
            "success": True,
            "start_date": adjusted_start_date,
            "end_date": adjusted_end_date,
        }
    except ValueError as ve:
        return {
            "success": False,
            "api_error": str(ve),
            "message": INVALIDDATEFORMAT,
            "translation_key": "INVALID_DATE_FORMAT",
        }

    except Exception as e:
        return {
            "success": False,
            "api_error": str(e),
            "message": "An unexpected error occurred",
            "translation_key": "UNEXPECTED_ERROR",
        }
