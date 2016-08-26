import os
import gzip

accessPath = "E:\\python_file\\accesslog"
dic={}
hit_status={}
frelist=[]
urlLogList=[]
fileList=os.listdir(accessPath)
Field=""
time_pos=3
url_pos=6

def get_access_num(url):
    access_num = 0
    dic_hit_time=hit_status[url]
    for hit_type in dic_hit_time:
        if hit_type == 'TCP_DENIED':
            continue
        if hit_type == 'TCP_MISS':
            continue
        access_num += len(dic_hit_time[hit_type])
    return access_num

def show_hit_status(url):
    dic_hit_time = hit_status[url]
    for hit_type in dic_hit_time:
        print(hit_type,":",len(dic_hit_time[hit_type]),end=' ')
    print()

def get_hit_type(line):   
    for item in line:
        if 'TCP_' in item:
             return item
    return '0'

def get_hit_type_b(line):
    for item in line:
        if b'TCP_' in item:
            return item.decode()
    return '0'
def get_hsd_score(url):
    score=0
    hsd_hit_score=0
    unhsd_hit_score=0
    miss_score=0
    other_score = 0
    dic_hit_time=hit_status[url]
    for key in dic_hit_time:
        if key == 'TCP_HSD_HIT' or key == 'TCP_IMS_HIT':
            hsd_hit_score+=len(dic_hit_time[key])
        elif 'MISS' in key:
            miss_score +=len(dic_hit_time[key])
        elif 'HIT' in key:
            unhsd_hit_score +=len(dic_hit_time[key])
        else:
            other_score+=len(dic_hit_time[key])
    # other data ,skip
    if hsd_hit_score+unhsd_hit_score == 0:
        return 0
    # 1 miss 1 hit
    if unhsd_hit_score == 1 and miss_score ==1:
        return 1000000

    score = hsd_hit_score*1000//(hsd_hit_score+unhsd_hit_score)
    score *=1000
    score +=other_score
    #score format 1000 000 #low 3 for unexpect status , high 4 for real hit rate
    return score
def check_score_status(url,score):
    if score == 0:
        if 'm3u8' in url:
            return 'NORMAL'
    if score / 1000 >= 999:
        return 'NORMAL'
    return 'LOW'
def check_if_1_hit(url):
    dic_hit_time=hit_status[url]

def show_log_file(url):
    urlLogList=[]
    for file in fileList:
        accesslog = os.path.join(accessPath,file)
        with gzip.open(accesslog) as f:
            for line in f:
                if url in line:
                    print(line) 
                    urlLogList.append(line)
def show_top_low_result(Num):
    showNum = 0
    for url,score in checklist:   
        if score != 'NORMAL':
            print(url," : ",score)
            show_hit_status(url)
            showNum += 1
        if showNum >= Num:
            break
#Analysis Begin
for file in fileList:
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
            i=i+1
        print("FileName:[",file,"]")
       # print("Get Total Request ",i)

#sort by most frequently reach
frelist=sorted([(key,get_access_num(key)) for key in hit_status],key = lambda d:d[1],reverse=True)
frelistZero=[(key,value) for key,value in frelist if value <=0]
frelistNoneZero=[(key,value) for key,value in frelist if value >0]
scorelist=[(url,get_hsd_score(url)) for (url,timet) in frelistNoneZero]
checklist=[(url,check_score_status(url,score)) for (url,score) in scorelist]

                

        

                



