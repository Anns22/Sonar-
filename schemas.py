from pydantic import BaseModel, validator
from typing import List, Optional
from datetime import date

from fastapi.exceptions import HTTPException


class PoolDateRangeCreate(BaseModel):
    start_date: date
    end_date: Optional[date] = None
    capacity: int

    @validator('end_date')
    def check_end_date(cls, end_date, values):
        if end_date is not None and end_date < values['start_date']:
            raise ValueError('end_date must be after start_date')
        return end_date

    @validator('capacity')
    def check_capacity(cls, capacity):
        if capacity < 0:
            raise HTTPException(
                detail='capacity must be non-negative', status_code=400)
        return capacity

    class Config:
        orm_mode = True



class PoolCreate(BaseModel):
    name: str
    remarks: Optional[str] = None
    date_ranges: List[PoolDateRangeCreate]

    class Config:
        orm_mode = True




class PoolUpdate(BaseModel):
    id: int
    check_date_ranges: bool
    name: Optional[str] = None
    remarks: Optional[str] = None
    date_ranges: Optional[List[PoolDateRangeCreate]] = None

    class Config:
        orm_mode = True

class ServicePoolsDelete(BaseModel):
    ids: List[int]