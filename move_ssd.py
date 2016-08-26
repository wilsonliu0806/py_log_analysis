import os
import datetime
import gzip
from perf import fn_timer
import json

def get_access_log_file(accessPathDir):
    fileList=os.listdir(accessPathDir)
    return fileList

def get_access_file_date(file_name):
    return datetime.datetime.strptime(file_name.split('_')[0][0:19],"%Y-%m-%d-%H-%M-%S")

def is_valid_time_file(moveout_date,pre_file_name,cur_file_name):
    if pre_file_name == '':
        file_date1 = moveout_date;
    else:
        file_date1 = get_access_file_date(pre_file_name)
    file_date2 = get_access_file_date(cur_file_name)
    return file_date1 <= moveout_date and  moveout_date < file_date2

def get_move_time(line):
    line0 = line[0]
    line1 = line[1].split('|')[0]
    move_time = datetime.datetime.strptime(line0+line1,"%Y/%m/%d%H:%M:%S")
    return move_time

def get_url_flow(line,num):
    flow = line.split()[num]

def search_url_flow(url,line,result):
    if url in line:
        urlflow = get_url_flow(line,num)
        urlresult = result.get(url)
        if urlresult == None:
            result[url] = urlflow
        else:
            result[url] += urlflow

def get_move_out_dic(path_ssd_move_log,dicMoveOut,dicMoveIn):
    with open(path_ssd_move_log) as f:
        for line in f:
            str = line.split()
            move_time = get_move_time(str)
            if str[3] == "MOVE_IN":
                dic=dicMoveIn
            else:
                dic=dicMoveOut
            list = dic.get(str[0])
            if list == None:
                dic[str[2]]=move_time
            else:
                dicException[str[2]]=move_time

#performance problem
@fn_timer
def build_url_startup_file(acessFileList,dicMoveOut,urlStartUp):
    pre_access_file = ''
    dicMoveOutModi = dicMoveOut;
    fileCnt = len(acessFileList);
    fileNo = 0
    for access_file in acessFileList:
        fileNo +=1
        print(fileNo,'/',fileCnt)
        dicMoveOutIter = dicMoveOutModi.copy()
        for url in dicMoveOutIter:
            if pre_access_file == '':
                if dicMoveOutModi[url] <  get_access_file_date(access_file):
                    urlStartUp[url]=access_file
                    del dicMoveOutModi[url]
                    continue
            if is_valid_time_file(dicMoveOutModi[url],pre_access_file,access_file):
                urlStartUp[url]=access_file
                del dicMoveOutModi[url]
                continue
        pre_access_file = access_file

def move_out_filter(start_date,end_date,dicMoveOut):
    dicMoveOutFilter = {}
    for url in dicMoveOut:
        if start_date <= dicMoveOut[url] and dicMoveOut[url] <= end_date:
            dicMoveOutFilter[url]=dicMoveOut[url]
    return dicMoveOutFilter

@fn_timer
def simplescanAccessLog(url,accessPath,acessFileList,result):
    for file in acessFileList:
        accesslog = os.path.join(accessPath,file)
        with gzip.open(accesslog) as f:
            burl = url.encode()
            if burl in f.read():
                print('url',burl,' exits in ',file)
                result[url]=file
#def scanAccessLog(url,accessPath,acessFileList):
@fn_timer
def scan_access_get_flow(url,accessPath,acessFileList,result):
    for file in acessFileList:
        accesslog = os.path.join(accessPath,file)
        with gzip.open(accesslog) as f:
            burl = url
            burl = burl.encode()
            for line in f:
                if burl in line:
                    print(burl)
                    rlturl = burl
                    rlturl = rlturl.decode()
                    sline = line
                    sline=sline.decode()
                    save_result(url,file,sline,result)

def save_result(url,accessFile,fileLine,result):
    urlitem = result.get(url)
    if urlitem ==  None:
        linestatus = []
        linestatus.append(fileLine)
        filestatus={}
        filestatus[accessFile]=linestatus;
        result[url]=filestatus
    else:
        filestatus = result[url].get(accessFile)
        if filestatus == None:
            linestatus = []
            linestatus.append(fileLine)
            result[url][accessFile]=linestatus
        else:
            result[url][accessFile].append(fileLine)
def mainfun():
    #build move out dic
    path_ssd_move_log = 'E:\\python_file\\ssd_move.log'
    path_access_log = 'E:\\python_file\\accesslog'
    dicMoveOut={}
    dicMoveIn={}
    get_move_out_dic(path_ssd_move_log,dicMoveOut,dicMoveIn)
    start_date = datetime.datetime(2016,7,24)
    end_date = datetime.datetime(2016,7,25)
    print('dicMoveOut len Before',len(dicMoveOut))
    dicMoveOut = move_out_filter(start_date,end_date,dicMoveOut)
    print('dicMoveOut len After',len(dicMoveOut))
    print('build move out dic finish!')
    #build access log dic
    accessPath = "E:\\python_file\\accesslog"
    acessFileList=get_access_log_file(accessPath)
    urlStartup=dicMoveOut
    print('build_url_startup_file start!')
    #build_url_startup_file(acessFileList,dicMoveOut,urlStartup)
    print('build_url_startup_file finish!')
    #scan access log
    print('dicMoveOut!',len(urlStartup))
    urlCnt = 0
    result={}
    for url in urlStartup:
        urlCnt +=1
        print('url ',urlCnt,'/',len(urlStartup))
        #simplescanAccessLog(url,path_access_log,acessFileList,result)
        scan_access_get_flow(url,path_access_log,acessFileList,result)
#Start Func
mainfun()










