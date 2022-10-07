# -*- coding: utf-8 -*-
"""
Created on Tue Sep  6 18:35:50 2022

@author: chuan
"""
#資料載入
import csv
import pandas as pd
from datetime import datetime

with open("data of 12 months 2020.csv","r") as csvfile:
    rows=csv.reader(csvfile)
    L=[]
    for row in rows:
        L.append(row)
data=pd.DataFrame(L[1:426888],columns=L[0])
data.info()
data.describe()

#修改欄位為日期屬性
data['started_at']=pd.to_datetime(data['started_at'],format="%d-%m-%Y %H:%M")
data['ended_at']=pd.to_datetime(data['ended_at'],format="%d-%m-%Y %H:%M")

#修改欄位為日期屬性(另一種方式)
data['started_at']=data['started_at'].astype('datetime64')
data['ended_at']=data['ended_at'].astype('datetime64')

#%% 

#Part1:租借長度
#利用租借總時間降冪排序(前五筆/後五筆)
data=data.sort_values('ride_length',ascending=False)
print(data.head(5))
print(data.tail(10)) 

#驗證有幾筆資料含有#字
data['ride_length'][0]
L1=[]
for i in data['ride_length']:
    L1.append(i)
L2=[]
for i in range(len(L1)):
    de=L1[i].find('#')
    if de!=-1:
        L2.append(de)
len(L2)

#str.contains()在Dataframe中的應用
df1=data.drop(data[data['ride_length'].str.contains('#')==True].index)
df1=df1.sort_values('ride_length',ascending=False)
df1.head() #前五筆
df1.tail() #後五筆 
#%%資料儲存
#Data存入SQLite資料庫
import sqlite3
conn=sqlite3.connect(r'C:\Users\chuan\Ride_Data.sqlite')
df1.to_sql('Clean_Data', conn,if_exists='replace',index=True)

conn.close()

#Data存入MySQL
import pymysql

# 連線資料庫
db1 = pymysql.connect(host = "127.0.0.1",
                      user = "root",
                      passwd = "12345678",
                      database = "bike_clean_data",
                      port = 3306)

cursor = db1.cursor()
cursor.execute("SELECT VERSION()")
data = cursor.fetchone()
print ("使用MySQLdb連線的資料庫版本為 : %s " % data)

#存入資料庫(mysql)
from sqlalchemy import create_engine
engine = create_engine('mysql+pymysql://root:12345678@localhost:3306/bike_clean_data')
df1.to_sql('clean_data_ver',engine,if_exists='replace',index=True)
db1.close()

#創建test資料表
sql1 = """CREATE TABLE EMPLOYEE_A (
          FIRST_NAME  CHAR(20) NOT NULL,
          LAST_NAME  CHAR(20),
          AGE INT,  
          SEX CHAR(1),
          INCOME FLOAT )"""

cursor.execute(sql1)
db1.close()
#%%資料分析
def data_mask(dataframe,column,time_1,time_2):
    m=dataframe[column]>=time_1
    m_1=dataframe[column]<=time_2
    number=dataframe[column][m&m_1].count()
    
    return number
data_mask(df1,'ride_length','00:01:00','00:15:00')

#這些騎乘時間長的紀錄，有特定分布在哪一些站點嗎?
def data_mask(dataframe,column_1,time_1,column_2):
    m=dataframe[column_1]>time_1
    number=dataframe[column_1][m].count()
    station=dataframe[m][column_2].value_counts()
    
    return number,station

data_mask(df1,'ride_length','12:00:00','start_station_name')

#會在同站點租借&歸還者的數目，有沒有特別分布在那些站點(因為長時間租借的很多都同站還)
def rent_return(dataframe,time_1,column_1,column_2,column_3):
    m=dataframe[column_1]>time_1
    number=dataframe[column_1][m].count()
    rent=dataframe[m][dataframe[m][column_2]==dataframe[m][column_3]].count()
    place=dataframe[m][column_2][dataframe[m][column_2]==dataframe[m][column_3]].value_counts()
    
    return number,rent,place

rent_return(df1,'01:00:00','ride_length','start_station_name','end_station_name')

#遮罩
mask=(df1['ride_length']>='00:01:00')
df1[mask] #419215

mask_1=(df1['ride_length']<='01:00:00') #短租條件一
short=df1['ride_length'][mask & mask_1].count() #短租 411769

mask_2=(df1['ride_length']>'03:00:00') #中租條件一
mask_3=(df1['ride_length']<'24:00:00') #中租條件二
df1[mask_2 & mask_3] #中租 #7036
df1[(df1['ride_length']>'01:00:00') & (df1['ride_length']<='12:00:00')] #第二種寫法
middle=df1['ride_length'][mask_2 & mask_3].count() 

mask_4=(df1['ride_length']>'12:00:00') #長租條件
df1[mask_4] #長租 #410
long=df1['ride_length'][mask_4].count()

#時間大多在一小時內。站點大多落在那幾站？
a=df1[mask_4].groupby('start_station_name').count()
a=a.sort_values('start_station_id',ascending=False)
a.head()
a.tail()

#同一天、同一組ID是否有再租借(少於1分鐘的ID)
mask_5=(df1['ride_length']>='00:01:00')
df1[mask_5]

for i in df1[mask]['ride_id']:
    for j in df1[mask_5]['ride_id']:
        if i == j:
            print('進行中')
            print(i)
            
#Part2:會員與非會員
#不重複使用者
df1[mask_5]['ride_id'] #419215
df1[mask_5]['ride_id'].nunique() #419199 重複者人數:16

test=df1[mask_5].drop_duplicates(subset='ride_id',inplace=False)
sum(test['member_casual']=='member') #374670
sum(test['member_casual']=='casual') #44529

#重複使用者中的會員/非會員比例
duplicate=df1[mask_5][df1[mask_5].duplicated(subset='ride_id')]
print(duplicate.loc[:,'ride_id':'member_casual'])

#Part3: DoW
#分布在哪一天多
df1[mask_5]['day_of_the_week'].value_counts()

#轉字串後再改為其他時間形式
L=[]
L1=[]
for i in df1['started_at']:
    time=i.split(' ')
    L.append(time[1])
    L1.append(time[0])
df1.insert(15,column='started_time',value=L)
df1.insert(16,column='started_date',value=L1) 

df1['started_date']=pd.to_datetime(df1['started_date'],format="%d-%m-%Y")

#%%
#資料視覺化
import seaborn as sns
import matplotlib.pyplot as plt

#騎乘總時間比例(三組，甜甜圈圖)
plt.figure(figsize=(4,4),dpi=150)

short=df1['ride_length'][mask & mask_1].count()
middle=df1['ride_length'][mask_2 & mask_3].count() 
long=df1['ride_length'][mask_4].count()

time_proportion=[short,middle,long]
x=['short','middle','']
colors=['#5e77dc','#92c2de','#ffffe0']
explode=[0,0,0.3]
plt.pie(time_proportion,labels=x,colors=colors,autopct='%1.1f%%',explode=explode)
plt.title("Proportion of users'ride time", {"fontsize" : 16})
plt.legend(labels=['short','middle','long'],loc=0,bbox_to_anchor=(9/10, 3/5))
plt.show()

#頻繁租借點/非頻繁租借點(bar chart)
plt.figure(dpi=300,figsize=(12,4))

y=['Columbus Dr & Randolph St',
   'Kingsbury St & Kinzie St',
   'Clinton St & Washington Blvd',
   'Clinton St & Madison St',
   'Canal St & Adams St']
x=[1600,3200,4800,6400,8000]
d=[a['start_station_id'][0],a['start_station_id'][1],a['start_station_id'][2],a['start_station_id'][3],a['start_station_id'][4]]
d=pd.DataFrame(d)
sns.barplot(x=x,y=y,data=d)

#不重複使用者/重複使用者比例；會員/非會員比例
plt.figure(figsize=(4,4),dpi=150)

non_uni=df1[mask]['ride_id'].nunique()
uni=(df1[mask]['ride_id'].count()-df1[mask]['ride_id'].nunique())

member=sum(df1[mask]['member_casual']=='member') #374670
casual=sum(df1[mask]['member_casual']=='casual') #44529
d=[member,casual]
x=['member','casual']
colors=['#0094b0', '#93003a']
explode=[0,0.1]
plt.pie(d,labels=x,colors=colors,autopct='%1.1f%%',explode=explode)
plt.title("Proportion of member & non-member", {"fontsize" : 14})
plt.legend(labels=['member','casual'],loc=0,bbox_to_anchor=(9/10, 3/5))
plt.show()

#DOW使用計數(bar chart)
plt.figure(dpi=300)

y=['Sunday',
   'Monday',
   'Saturday',
   'Friday',
   'Tuesday',
   'Thursday',
   'Wednesday']
x=[20000,30000,40000,50000,60000,70000,80000]
d=[sum(df1[mask]['day_of_the_week']=='3'),
   sum(df1[mask]['day_of_the_week']=='4'),
   sum(df1[mask]['day_of_the_week']=='2'),
   sum(df1[mask]['day_of_the_week']=='5'),
   sum(df1[mask]['day_of_the_week']=='6'),
   sum(df1[mask]['day_of_the_week']=='1'),
   sum(df1[mask]['day_of_the_week']=='7')]
d=pd.DataFrame(d)

color=['#6677a7','#7395b5','#6677a7''#bacaff','#bacaff','#bacaff','#bacaff']
sns.barplot(x=x,y=y,data=d,color=color)

#有效次數折線圖變化(1~3月)
L1=[]
for i in df1['started_at']:
    j=i.split('-')
    L1.append(j[1])
L2=[]
for i in L1:
    j=i.split('0')
    L2.append(int(j[1]))

df1.insert(15,column='month',value=L2)

plt.figure(dpi=300)
y=[sum(df1[mask]['month']==1),
   sum(df1[mask]['month']==2),
   sum(df1[mask]['month']==3)]
x=['Jan 2020','Feb 2020','Mar 2020']
plt.style.use('ggplot') #灰階背景
ax = sns.lineplot(x=x, y=y,color='#7d7d89')
ax.set_yticks(ticks=[125000,130000,135000,140000,145000])
ax.set_title('Ride records(by month)')

for x_1,y_1 in zip(x,y):
    plt.text(x_1, y_1+0.8, str(y_1), ha='center', va='bottom', color='white',fontsize=10,rotation=0).set_backgroundcolor('#7d7d89')


#會員與非會員的關係(bar)
plt.figure(dpi=300)

member=[sum(df1[mask&mask_1]['member_casual']=='member'),
        sum(df1[mask_2&mask_3]['member_casual']=='member'),
        sum(df1[mask_4]['member_casual']=='member')]
casual=[sum(df1[mask&mask_1]['member_casual']=='casual'),
        sum(df1[mask_2&mask_3]['member_casual']=='casual'),
        sum(df1[mask_4]['member_casual']=='casual')]

x=['Short','Middle','Long']

ax=sns.barplot(x=x,y=member,color='#61b8d8')
ax=sns.barplot(x=x,y=casual,color='#d58e96')
ax.set_title('member or casual(by ride time)')

#會員與非會員的關係(pie)
#short
plt.figure(dpi=300)

short=[member[0],casual[0]] #373635 #38134
seperate=[0,0.08]
plt.pie(short,labels=['member','casual'],colors=['#61b8d8','#d58e96'],autopct='%1.1f%%',explode=seperate)
plt.axis('equal')
plt.title('Ride Time(Short)')
plt.show()

#middle
plt.figure(dpi=300)

middle=[member[1],casual[1]] #942 #6094
seperate=[0,0.08]
ax=plt.pie(middle,labels=['member','casual'],colors=['#61b8d8','#d58e96'],autopct='%1.1f%%',explode=seperate)
plt.axis('equal')
plt.title('Ride Time(Middle)')
plt.show()

#long
plt.figure(dpi=300)

long=[member[2],casual[2]] #109 #301
seperate=[0,0.04]
ax=plt.pie(long,labels=['member','casual'],colors=['#61b8d8','#d58e96'],autopct='%1.1f%%',explode=seperate)
plt.axis('equal')
plt.title('Ride Time(Long)')
plt.show()

#距離與騎乘時間關係(散佈圖)
#從資料庫讀取資料
def read_sql(host,user,passwd,database,port,e):
    import pymysql
    db1 = pymysql.connect(host=host,
                          user=user,
                          passwd=passwd,
                          database=database,
                          port=port)
    cursor=db1.cursor()
    cursor.execute(e)
    data=cursor.fetchall()
    
    return data
read_sql('127.0.0.1','root','12345678','bike_clean_data',3306,
         '''SELECT * FROM clean_data''')

import pymysql
db1 = pymysql.connect(host = "127.0.0.1",
                      user = "root",
                      passwd = "12345678",
                      database = "bike_clean_data",
                      port = 3306)

cursor = db1.cursor()
cursor.execute('''SELECT * FROM clean_data
WHERE (ride_length >= '00:01:00')''')
data=cursor.fetchall()
L1=['index']
for i in range(len(L[0])):
    L1.append(L[0][i])
    
data=pd.DataFrame(data,columns=L1)

#計算距離
from math import radians,cos,sin,asin,sqrt
def haversine(lon1,lat1,lon2,lat2): #開始經度/開始緯度/結束經度/結束緯度

    lon1,lat1,lon2,lat2=map(radians,[lon1,lat1,lon2,lat2])
    
    dlon=lon2-lon1
    dlat=lat2-lat1
    a=sin(dlat/2)**2+cos(lat1)*cos(lat2)*sin(dlon/2)**2
    c=2*asin(sqrt(a))
    r=6371
    
    return c*r*1000

haversine(float(data['start_lng'][0]),float(data['start_lat'][0]),
          float(data['end_lng'][0]),float(data['end_lat'][0]))
L2=[]
for i in range(len(data)):
    L2.append(haversine(float(data['start_lng'][i]),float(data['start_lat'][i]),
              float(data['end_lng'][i]),float(data['end_lat'][i])))
L3=[]
for i in L2:
    try:
        j=round(i,2) #取至小數點第二位
        L3.append(j)
    except:
        L3.append(i)
        pass

data.insert(16,column='distance',value=L3)

#散佈圖
import pandas as pd
import seaborn as sns

plt.figure(dpi=300,figsize=[12,4])
ax = sns.scatterplot(x=data['distance'], y=data['ride_length'], hue=data['member_casual'])
ax.set_title('ride_length & ride_distance')

plt.figure(dpi=300)
ax = sns.scatterplot(x=data['ride_length'], y=data['distance'], hue=data['member_casual'])
ax.set_title('Scatter Chart')


data['ride_length']=(data['ride_length'].dt.seconds)/3600
sum(data['ride_length'])/419215

#%% GDS
#計算站點使用次數
a=df1[mask_5].groupby('start_station_name').count()
a=a.sort_values('start_station_id',ascending=False)
a.head()
a.tail()

#計算有幾個站點(不重複)
df1[mask_5]['start_station_name'].nunique()

#%%
# #Test
#Test=[['ID','Name','Gender','content'],[1,'Mary','Female','###'],[2,'Jason','Male','forty'],[3,'Sandy','Female','fifty'],[4,'William','Male','##'],[5,'Antony','Male','%'],[6,'Quinee','Female','#'],[3,'William','Male','##'],[2,'Quinee','Female','#']]
#df=pd.DataFrame(Test[1:9],columns=Test[0])



# mas_k=df['ID']<4
# ma_sk=df['ID']>=4

# for j in df[ma_sk]['Name']:
#     for i in df[mas_k]['Name']:
#         if j==i:
#             print(i)

# df_1=df.drop(df[df['content'].str.contains('#')==True].index)

