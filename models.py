from sqlalchemy import Column, Integer, String, Text, Date, ForeignKey, TIMESTAMP, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import TINYINT
from datetime import datetime


Base = declarative_base()


class Pool(Base):
    __tablename__ = 'pools'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True,index=True)
    remarks = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, nullable=False,
                        server_default='CURRENT_TIMESTAMP')
    updated_at = Column(
        TIMESTAMP, server_default='CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')
    created_by_id = Column(Integer, nullable=False)
    updated_by_id = Column(Integer, nullable=True)
    subscriber_id = Column(Integer, primary_key=True,
                           nullable=False, index=True)
    deleted = Column(TINYINT(1), nullable=False, server_default=text("'0'"))
    record_status = Column(TINYINT(1), nullable=False,
                           server_default=text("'1'"))

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'remarks': self.remarks,
            'created_by_id': self.created_by_id,
            'updated_by_id': self.updated_by_id,
            'subscriber_id': self.subscriber_id,
            'deleted': self.deleted,
            'record_status': self.record_status
        }


class PoolDateRange(Base):
    __tablename__ = 'pool_date_ranges'

    id = Column(Integer, primary_key=True, autoincrement=True)
    pool_id = Column(Integer, nullable=False, index=True)
    start_date = Column(Date, nullable=False, index=True)
    end_date = Column(Date, nullable=True, index=True)
    capacity = Column(Integer, nullable=False)
    created_at = Column(TIMESTAMP, nullable=False,
                        server_default='CURRENT_TIMESTAMP')
    updated_at = Column(
        TIMESTAMP, server_default='CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')
    created_by_id = Column(Integer, nullable=False)
    updated_by_id = Column(Integer, nullable=True)
    subscriber_id = Column(Integer, primary_key=True,
                           nullable=False, index=True)
    deleted = Column(TINYINT(1), nullable=False, server_default=text("'0'"))
    record_status = Column(TINYINT(1), nullable=False,
                           server_default=text("'1'"))

    def to_dict(self):
        return {
            'id': self.id,
            'pool_id': self.pool_id,
            'start_date': self.start_date.strftime('%Y-%m-%d') if self.start_date else None,
            'end_date': self.end_date.strftime('%Y-%m-%d') if self.end_date else None,
            'capacity': self.capacity,
            'created_by_id': self.created_by_id,
            'updated_by_id': self.updated_by_id,
            'subscriber_id': self.subscriber_id,
            'deleted': self.deleted,
            'record_status': self.record_status
        }


class PoolDateRangeHistory(Base):
    __tablename__ = 'pool_date_range_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    pool_id = Column(Integer, nullable=False)
    pool_date_range_id = Column(Integer, nullable=False)
    action_type = Column(String(50), nullable=False)
    old_start_date = Column(Date, nullable=True)
    old_end_date = Column(Date, nullable=True)
    old_capacity = Column(Integer, nullable=True)
    subscriber_id = Column(Integer, nullable=False)
    deleted = Column(TINYINT(1), nullable=False, server_default=text('0'))
    record_status = Column(TINYINT(1), nullable=False,
                           server_default=text('1'))
    created_at = Column(TIMESTAMP, nullable=False,
                        server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(TIMESTAMP, server_default=text(
        'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    created_by_id = Column(Integer, nullable=False)
    updated_by_id = Column(Integer, nullable=True)
