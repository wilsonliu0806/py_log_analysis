# -*- coding: utf-8 -*-
"""
    future.py
    ~~~~~~~~~~~~~

    find url in accesslogs and calc the flow after move out of the HSD.

    :copyright: (c) 2016 by liuw1.
    :license: BSD, see LICENSE for more details.
"""
import os
import datetime
import gzip
import json
from config import *
def get_access_log_file(accessPathDir):
    '''list all the file name giving dir'''
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

def search_url_flow(url,result):
    flow_total = 0
    logdetail = result[url]
    for logfile in logdetail:
        linelist = logdetail[logfile]
        for line in linelist:
            linesplit = line.split()
            try:
                flow_total += int(linesplit[9])
            except:
                print(linesplit)
            else:
                continue
    print(url,'Total Flow',flow_total)
    return flow_total

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

url_pos=6
#modify later
def url_scan_access_get_flow(url,accessPath,acessFileList,result):
    fileCnt = len(acessFileList);
    fileNo = 0
    for file in acessFileList:
        fileNo += 1
        print(fileNo,'/',fileCnt)
        accesslog = os.path.join(accessPath,file)
        with gzip.open(accesslog) as f:
            for line in f:  
                lineurl = line
                lineurlsplit=lineurl.split()
                if len(lineurlsplit) < 6:
                    continue
                if 'default-url' in file:
                    lineurl = lineurlsplit[6]
                    try:
                        lineurl = lineurl.decode()
                        lineurlsplit = lineurl.split('?')
                    except:
                        print(lineurl)
                    else:
                        lineurl = lineurlsplit[0][1:]
                        lineurl = lineurl.encode()
                else:
                    lineurl = lineurlsplit[6]
                    if b'?' in lineurl:
                        lineurl = lineurl.decode()
                        lineurlsplit=lineurl.split('?')
                        lineurl = lineurlsplit[0]
                        lineurl = lineurl.encode()
                if lineurl in url:
                    save_result(lineurl,file,line,result)
               
def simplescanAccessLog(url,accessPath,acessFileList,result):
    fileCnt = len(acessFileList);
    fileNo = 0
    for file in acessFileList:
        fileNo +=1
        print(fileNo,'/',fileCnt)
        accesslog = os.path.join(accessPath,file)
        with gzip.open(accesslog) as f:
            data = f.read()
            for burl in url:
                if burl in data:
                    print('url',burl,' exits in ',file)
                    result[burl]=file
#def scanAccessLog(url,accessPath,acessFileList):
def scan_access_get_flow(url,accessPath,acessFileList,result):
    fileCnt = len(acessFileList);
    fileNo = 0
    for file in acessFileList:
        fileNo +=1
        print(fileNo,'/',fileCnt)
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
    strfileline = fileLine.decode()
    if urlitem ==  None:
        linestatus = []
        linestatus.append(strfileline)
        filestatus={}
        filestatus[accessFile]=linestatus;
        result[url]=filestatus
    else:
        filestatus = result[url].get(accessFile)
        if filestatus == None:
            linestatus = []
            linestatus.append(strfileline)
            result[url][accessFile]=linestatus
        else:
            result[url][accessFile].append(strfileline)
def format_result(result):
    strresult = {url.decode():result[url] for url in result}
    return strresult

def mainfun():
    #build move out dic
    dicMoveOut={}
    dicMoveIn={}
    get_move_out_dic(path_ssd_move_log,dicMoveOut,dicMoveIn)
    print('dicMoveOut len Before',len(dicMoveOut))
    dicMoveOut = move_out_filter(start_date,end_date,dicMoveOut)
    print('dicMoveOut len After',len(dicMoveOut))
    print('build move out dic finish!')
    #build access log dic
    accessPath = "E:\\python_file\\accesslog"
    acessFileList=get_access_log_file(accessPath)
    print('build_url_startup_file start!')
    #build_url_startup_file(acessFileList,dicMoveOut,urlStartup)
    urlStartup={url.encode():'1' for url in dicMoveOut }
    print('build_url_startup_file finish!')
    #scan access log
    print('dicMoveOut!',len(urlStartup))
    urlCnt = 0
    resulturl={}
    resultsimple={}
    print('url_scan_access_get_flow start!')
    #url_scan
    url_scan_access_get_flow(urlStartup,path_access_log,acessFileList,resulturl)
    print('result url len ',len(resulturl))
    strresult = format_result(resulturl)
    fpurl = open('resulturl.json','w')
    json.dump(strresult,fpurl)
    '''
    #simple_scan
    simplescanAccessLog(urlStartup,path_access_log,acessFileList,resultsimple)
    print('url_scan_access_get_flow end!')
    strresult = format_result(resultsimple)
    fp = open('resultsimple.json','w')
    json.dump(strresult,fp)
    '''
def load_and_statics(url):
    fpurl = open('resulturl.json','r')
    resulturl = json.load(fpurl)
    search_url_flow(url,resulturl)

#Start Func
if __name__ == '__main__':
    Sel = input("1 for create json,2 for load json and analysis")
    if Sel == '1':
        mainfun()
    fpurl = open('resulturl.json','r')
    resulturl = json.load(fpurl)
    flowTotal = 0
    for url in resulturl:
        urlTotal=search_url_flow(url,resulturl)
        flowTotal+=urlTotal
    print(flowTotal)

'''
with concurrent.futures.ThreadPoolExecutor(max_workers = 5) as executor:
    future_to_url = {executor.submit(load_url,url,60):url for url in URLS}
    for future in concurrent.futures.as_completed(future_to_url):
        url = future_to_url[future]
        try:
            data = future.result()
        except Exception as exec:
            print
'''
