import getopt,sys
import datetime

from py_squidlog import data 
from py_squidlog.data.db_modele import AccessLogStatis
from data.db_access_log import import_access_log
from data.db_conn import engine,Session

def useage():
    print("log_ana !")

def analysis():
    conn = engine.connect()
    sess = Session()
    time_step = 5
    times = 24*60/5
    #while end_date <datetime.datetime(2017,1,25,0,0,0):
    for i in range(times):
        start_date=datetime.datetime(2017,1,24,0,0,0)+datetime.timedelta(0,time_step*60*i)
        end_date = datetime.datetime(2017,1,24,0,0,0)+datetime.timedelta(0,time_step*60*(i+1))
        print(end_date)
        sql="select hit,sum(size)/1024/1024/1024 as size,count(*) from access_log_raw where date >='"\
        +str(start_date)+"' and date <'"+str(end_date)\
        +"' group by hit"
        record = conn.execute(sql)
        total = 0 
        total_tcp_hit =0 
        total_tcp_hsd_hit =0 
        total_tcp_par_hit =0 
        statit_date = end_date 
        for line in record:

            total+=line[1]

            if 'TCP_HSD_HIT' in line[0]:
                total_tcp_hsd_hit = line[1]
            if 'TCP_HIT' in line[0]:
                total_tcp_hit = line[1]
            if 'TCP_PART_HIT' in line[0]:
                total_tcp_par_hit = line[1]

        statistic=AccessLogStatis(total=total,total_tcp_hit = total_tcp_hit\
        ,total_tcp_hsd_hit=total_tcp_hsd_hit,total_tcp_par_hit=total_tcp_par_hit,statit_date=end_date)
        sess.add(statistic)
        sess.commit()
        



def main(argv):
    try:
        opts,args = getopt.getopt(argv[1:],"hi:",["help","import"])
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)
    table_name=""
    for o,a in opts:
        if o in ("-h","--help"):
            useage()
            sys.exit()
        elif o in ("-i","--import"):
            table_name = a
            print("output %s"%table_name)
    if len(table_name) >0:
        if table_name in "access_log_raw":
            print("import start! ")

if __name__ == '__main__':
    print(__name__)
    print(__package__)
    #import_access_log()
    analysis()