import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table,Column,Integer,String,MetaData,ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import DateTime

import os
import gzip
import datetime


Base = declarative_base()
class StoreStat(Base):

    __tablename__='store_stat'

    store_stat_id = Column(Integer,primary_key=True)

    url = Column(String(1024))
    path = Column(String(256))
    file_sz = Column(Integer)
    timestamp = Column(DateTime)
    lastref = Column(DateTime)
    refcount = Column(Integer)

class AccessLog(Base):

    __tablename__="access_log"

    access_log_id  = Column(Integer,primary_key=True)
    url_id = Column(Integer)
    hit = Column(Integer)
    date = Column(DateTime)

    def __repr__(self):
        return "<date %s url_id %d status %d >" %(date,url_id,hit)
    
class AccessLogRaw(Base):

    __tablename__='access_log_raw'

    access_log_id  = Column(Integer,primary_key=True)
    url = Column(String(1024))
    hit = Column(String(32))
    date = Column(DateTime)

    def __repr__(self):
        return "<date %s url_id %s status %d >" %(date,url,hit)
engine = create_engine('mysql+mysqldb://root:root@localhost/squidlog')
metadata = MetaData(engine)
Base.metadata.create_all(engine)
Session = sessionmaker(bind = engine)
sess = Session() 
month_cvt = {'Jan':'1','Feb':'2','Mar':'3','Apr':'4','May':'5','Jun':'6','Jul':'7'\
,'Aug':'8','Sep':'9','Oct':'10','Nov':'11','Dec':'12'}

def get_hit_type_b(line):
    for item in line:
        if b'TCP_' in item:
            return item.decode()
    return '0'
def convert_time(time_str):
    time_str = time_str.decode()
    year = time_str[7:11]
    month = month_cvt[time_str[3:6]]
    day = time_str[0:2]
    hour = time_str[12:14]
    minutes = time_str[15:17]
    sec = time_str[18:20]
    return datetime.datetime(int(year),int(month),int(day),int(hour),int(minutes),int(sec))

def import_access_log():
    accessPath = "E:\\python_file\\accesslog"
    dic={}
    hit_status={}
    frelist=[]
    urlLogList=[]
    fileList=os.listdir(accessPath)
    Field=""
    time_pos=3
    url_pos=6
    #Analysis Begin
    for file in fileList:
        file_cnt = 0
        if file[0] == '.':
            continue 
        if 'default-url' in file:
            continue
        if 'qqvideo' in file:
            continue
        if 'vod' in file:
            continue
        accesslog = os.path.join(accessPath,file)  
        with gzip.open(accesslog) as f:
        #with open(accesslog) as f: 
            file_cnt +=1
            i = 0
            for line in f:
                j=0
                if i == 1:
                    #skip line 1
                    i=i+1
                    continue
                str = line.split()   
                strlen=len(str)  
                #fileter line less than 7
                if strlen < 7:
                    continue  
                #hit status as {hit_type,time}  
                #import item
                if 'gz' in accesslog:
                    url = str[url_pos].decode()
                    hit_type = get_hit_type_b(str)
                    time_detail = str[time_pos].decode()
                else:
                    url = str[url_pos]
                    hit_type = get_hit_type(str)
                    time_detail = str[time_pos]
                #import item end
                status = hit_status.get(url)
                if status == None:
                    dic_hit_time={}
                    dic_hit_time[hit_type]=[time_detail]
                    hit_status[url]=dic_hit_time
                else:
                    dic_hit_time = status.get(hit_type)
                    if dic_hit_time == None:
                        hit_status[url][hit_type]=[time_detail]
                    else:
                        hit_status[url][hit_type].append(time_detail)
                if(len(time_detail)<10):
                    continue
                time_detail = time_detail[1:]
                print(time_detail)
                time_detail = convert_time(time_detail)
                access_detail = AccessLogRaw(url = url,hit=hit_type,date=time_detail)
                i=i+1
                sess.add(access_detail)
                if(i == 10):
                    sess.commit()
                    break
            print("FileName:[",file,"]")
            if(file_cnt == 2):
                break
        # print("Get Total Request ",i)
def import_store_stat():
    store_stat_path="E:\\python_file\\store_stat"
