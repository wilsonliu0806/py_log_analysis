import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table,Column,Integer,BigInteger,String,MetaData,ForeignKey,Float
from sqlalchemy.types import DateTime
from sqlalchemy.dialects.mysql import INTEGER
from db_conn import engine
Base = declarative_base()

class StoreStat(Base):

    __tablename__='store_stat'

    store_stat_id = Column(Integer,primary_key=True)

    url = Column(String(1024))
    path = Column(String(256))
    cur_file_sz = Column(BigInteger)
    swap_file_sz = Column(BigInteger)
    timestamp = Column(DateTime)
    lastref = Column(DateTime)
    refcount = Column(Integer)

class AccessLog(Base):

    __tablename__="access_log"

    access_log_id  = Column(Integer,primary_key=True)
    url_id = Column(Integer)
    hit = Column(Integer)
    size = Column(Integer)
    date = Column(DateTime)

    def __repr__(self):
        return "<date %s url_id %d status %d >" %(date,url_id,hit)
    
class AccessLogRaw(Base):

    __tablename__='access_log_raw'

    access_log_id  = Column(Integer,primary_key=True)
    url = Column(String(4096))
    hit = Column(String(32))
    size = Column(Integer)
    date = Column(DateTime)

    def __repr__(self):
        return "<date %s url_id %s status %d >" %(date,url,hit)

class AccessLogList(Base):

    __tablename__='access_log_list'

    access_log_file_id = Column(Integer,primary_key=True)
    file_name = Column(String(1024))
    state = Column(Integer)
    start_time = Column(DateTime)
    end_time = Column(DateTime)

class AccessLogStatis(Base):

    __tablename__="access_log_statis"

    access_log_statis_id = Column(Integer,primary_key=True)
    total = Column(Float)
    total_tcp_hit = Column(Float)
    total_tcp_hsd_hit = Column(Float)
    total_tcp_par_hit = Column(Float)
    statit_date = Column(DateTime)

class MoveSsdLog(Base):

    __tablename__="move_ssd_log"

    move_ssd_log_id = Column(Integer,primary_key=True)
    url = Column(String(4096))
    move = Column(String(16))
    size = Column(BigInteger)
    date = Column(DateTime)



