
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
#spark.conf.set("spark.sql.execution.arrow.enabled","true")
import requests
from bs4 import BeautifulSoup
import html5lib
import time
import datetime
import pandas as pd
import numpy as np
from datetime import timedelta
import s3fs



start_time=time.time()
team_dict={1:'England',2:'Australia',3:'South Africa',4:'West Indies',5:'New Zealand',6:'India',7:'Pakistan',8:'Sri Lanka',9:'Zimbabwe',25:'Bangladesh',29:'Ireland',40:'Afghanistan',15:'Netherlands',30:'Scotland',32:'Nepal',27:'United Arab Emirates',20:'Papua New Guinea',17:'Canada',26:'Kenya',11:'United States of America'}

request_link1='http://www.espncricinfo.com/ci/content/player/caps.html?country='
request_link3=';class=1'
for request_no in[40,2,25,1,6,29,5,7,3,8,4,9]:
    print(team_dict.get(request_no))
    team_name=team_dict.get(request_no)
    request_link2=str(request_no)
    req_final_link=request_link1+request_link2+request_link3
    r = requests.get(req_final_link)

    soup = BeautifulSoup(r.text, "html.parser")
    links = soup.find_all("a")

    tags = soup.find_all("a","ColumnistSmry")
    #data=soup.find('a').text()


    link1='http://stats.espncricinfo.com/ci/engine/player/'
    link3='.html?class=1;template=results;type=allround;view=match'

    link4='.html?class=1;template=results;type=allround;view=innings'

    l=[]
    numlist=[]
    n=0
    inn1=[]
    inn2=[]
    cnt=1
    length=(len(tags))
    fd_ind_test=pd.DataFrame()
    for i in range(n,length,2):
        s=str(tags[i])
        nm=tags[i].get_text()
        #print(s)
        no=s[50:57]
    
        num=no.replace('.','')
        num1=num.replace('h','')
        num2=num1.replace('t','')
        link2=str(num2)
        l.append([num2,nm])
    
        link2=str(num2)
        flink_test=link1+link2+link3       #match link
        flink_test1=link1+link2+link4      #innings link
        end_time=time.time()
        total_time=datetime.timedelta(seconds=(round(end_time-start_time,2)))
        print(flink_test," ",cnt," ",team_name," ",total_time)
        cnt+=1
        ind_test=pd.read_html(flink_test,header=None)     # match data
    
        #ind_test_inn=pd.read_html(flink_test1,header=None)  #innings data
    
        ind_df_test=ind_test[3]
        #ind_df_test_inn=ind_test_inn[3]
        ind_df_test.index = np.arange(1,len(ind_df_test)+1)
        #llist=[]
    #llist=ind_df_test_inn['Overs']
        #ll=len(llist)
        #for i in range(0,ll,1):
            #inn2.append(llist[i])
    #ind_df_test['Over1']=inn1
    #ind_df_test['Over2']=inn2
        ind_df_test['Player_id']=num2
        ind_df_test['Name']=nm
    
        fd_ind_test=pd.concat([fd_ind_test,ind_df_test])
        
        #path='G:\\Machine Learning\\finaldataset\\test\\'+team_name+'.csv'
        #fd_ind_test.to_csv(path,mode='a')
        time.sleep(1.5)
#fd_ind_test['over1']=inn1
#fd_ind_test['over2']=inn2
    #fd_ind_test['Country']=team_name
    #path='G:\\Machine Learning\\finaldataset\\test\\'+team_name+'.csv'
    #team_no=str(request_no)
    fd_ind_test['Country']=team_name
    team_no=str(request_no)
    #path2='E:\\odi\\'+team_name+team_no+'.csv'
    #path3='/home/hadoop/cricket/odi/'+team_name+team_no+'.csv'
    #fd_ind_test.to_csv(path3)
    #s3 = s3fs.S3FileSystem(anon=False)
    team_name=team_name.replace(" ","")
    ts=time.time()
    fd_ind_test['Wkts']=fd_ind_test['Wkts'].astype(str)
    fd_ind_test['Conc']=fd_ind_test['Conc'].astype(str)
    fd_ind_test['Ct']=fd_ind_test['Ct'].astype(str)
    fd_ind_test['St']=fd_ind_test['St'].astype(str)
    fd_ind_test['Bat1']=fd_ind_test['Bat1'].astype(str)
    fd_ind_test['Bat2']=fd_ind_test['Bat2'].astype(str)
    fd_ind_test['Runs']=fd_ind_test['Runs'].astype(str)
	fd_ind_test['Start Date']=fd_ind_test['Start Date'].astype(str)
    
    #fd_ind_test['Unnamed: 9']=fd_ind_test['Bat1'].astype(str)
    
    #fd_ind_test.drop(['Unnamed: 5'],axis=1)
    #fd_ind_test['Wkts']=pd.to_numeric(fd_ind_test['Wkts'])
    #fd_ind_test['Conc']=pd.to_numeric(fd_ind_test['Conc'])
    #fd_ind_test['Ct']=pd.to_numeric(fd_ind_test['Ct'])
    #fd_ind_test['St']=pd.to_numeric(fd_ind_test['St'])
    #fd_ind_test['Runs']=pd.to_numeric(fd_ind_test['Runs'])
    #fd_ind_test['Wkts'] = pd.to_numeric(fd_ind_test['Wkts'], errors='coerce')
    time_stamp=str(datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d'))
    spark_df = sqlContext.createDataFrame(fd_ind_test)
    spark_df.coalesce(1).write.mode("append").option("header","true").format("csv").save("s3a://projectespn/TEST/"+team_name+"/"+time_stamp+"/")
    #with s3.open(path3,'w',encoding='utf-8') as f:
       # fd_ind_test.to_csv(f)    

#fd_ind_test.to_csv("G:\\Machine Learning\\finaldataset\\test\\all_teams.csv")


