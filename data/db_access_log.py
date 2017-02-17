import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db_modele import *
import os
import gzip
import datetime
import sys



engine = create_engine('mysql+mysqldb://root:root@localhost/squidlog')
metadata = MetaData(engine)
Base.metadata.create_all(engine)
Session = sessionmaker(bind = engine)
sess = Session() 
month_cvt = {'Jan':'1','Feb':'2','Mar':'3','Apr':'4','May':'5','Jun':'6','Jul':'7'\
,'Aug':'8','Sep':'9','Oct':'10','Nov':'11','Dec':'12'}

start_file = '0' 
start_read = False
start_line = -1 
start_line_read = False

for i in range(1,len(sys.argv)):
    if(i == 1):
        start_file = sys.argv[i]
        print(start_file)
    if(i == 2):
        start_line = int(sys.argv[i])
        print(start_line)

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
    size_pos=9
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
            print("FileName:[",file,"]")
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
                    url = str[url_pos]#.decode()
                    hit_type = get_hit_type_b(str)
                    size = str[size_pos]
                    time_detail = str[time_pos].decode()
                else:
                    url = str[url_pos]
                    hit_type = get_hit_type(str)
                    size = str[size_pos]
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
                try:
                    time_detail = convert_time(time_detail)
                except:
                    pass
                if(len(url)>1024):
                    continue
                access_detail = AccessLogRaw(url = url,hit=hit_type,date=time_detail,size=size)
                i=i+1
                sess.add(access_detail)
                if(i%100 == 0):
                    sess.commit()
            sess.commit()

def import_store_stat():

    start_read = True
    start_line_read = True 
    store_stat_path="E:\\python_file\\store_stat"
    filelist = os.listdir(store_stat_path)
    for file in filelist:
        print(file)
        if start_file in file:
            start_read=True
            print("File Start read")
        if not start_read:
            print(file)
            print("Skip")
            continue
        store_stat_log = os.path.join(store_stat_path,file)
        print(store_stat_log)
        with open(store_stat_log) as f:
            linenum = 0
            for line in f:
                linenum +=1
                if not start_line_read:
                    if(linenum > start_line):
                        print("Start Reading...")
                        start_line_read = True
                if (start_line_read == False):
                    continue
                strline = line.split(' ')
                if(len(strline)<7):
                    print(strline)
                    continue
                url = strline[0]
                path = strline[1]
                cur_sz = strline[2]
                swap_sz = strline[3]
                timestamp = datetime.datetime.fromtimestamp(int(strline[4]))
                lastref = datetime.datetime.fromtimestamp(int(strline[5]))
                refcount = strline[6]
                store_stat_detail = StoreStat(url=url,path=path,cur_file_sz=cur_sz,swap_file_sz = swap_sz\
                ,timestamp = timestamp,lastref=lastref,refcount=refcount)
                sess.add(store_stat_detail)
                if(linenum % 100 == 0):
                    print(linenum)
                    sess.commit()

if __name__ == "__main__":
    #import_store_stat()
    import_access_log()
                



