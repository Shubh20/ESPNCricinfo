import pandas as pd
#import pyarrow.orc as orc
from datetime import datetime
from pyspark.context import SparkContext, SparkConf
#conf=SparkConf.setAppName("pyspark")
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
from pyspark.sql import SQLContext
sqlContext=SQLContext(sc)
spark = SparkSession(sc)

#empty_df=spark.createDataFrame()
df=pd.DataFrame()


list1=['WestIndies','Pakistan','Zimbabwe','Bangladesh','Ireland','Netherlands','Scotland','Nepal','UnitedArabEmirates','PapuaNewGuinea','Canada','Kenya','UnitedStatesofAmerica','Afghanistan','India','Australia','England','SriLanka','NewZealand','SouthAfrica',]
for i in list1:
    s=str(i)
    spark_df1=spark.read.orc("s3a://projectespn/T20/"+s+"/*/*.orc")
    #df=empty_df.union(spark_df1)
    pandas_df=spark_df1.toPandas()
    df=pd.concat([df,pandas_df])

#print(new_pandas_df.iloc[:,5:12].head(300))
df['NOTOUT']=df['Bat1'].str.endswith('*').map({True:1,False:0})
df['Bat1']=df['Bat1'].str.replace('*','')

df.rename(columns={"Bat1":"Runs","Unnamed: 9":"T20_id","Start Date":"Match_Date","Ct":"Catches","St":"Stumpings"},inplace=True)

print(list(df.columns))

df['Runs']=df['Runs'].replace('TDNB','DNB')
df['Runs']=df['Runs'].replace('DNB','')
df['Runs']=df['Runs'].replace('absent','')
df['Runs']=df['Runs'].replace('sub','')

df.drop(columns=['Unnamed: 5'],inplace=True)

df['Opposition']=df['Opposition'].str.replace("v ","")
df['T20_id']=df['T20_id'].str.replace("T20I # ","")

df['Runs']=pd.to_numeric(df['Runs'])#.astype(int)
df['Wkts']=pd.to_numeric(df['Wkts'])#.astype(int)
df['Conc']=pd.to_numeric(df['Conc'])#.astype(int)
df['Catches']=pd.to_numeric(df['Catches'])#.astype(int)
df['Stumpings']=pd.to_numeric(df['Stumpings'])#.astype(int)
df['Opposition']=df['Opposition'].astype(str)
df['Ground']=df['Ground'].astype(str)
df['Match_Date']=df['Match_Date'].astype(str)#,format='%d-%b-%Y')
df['T20_id']=pd.to_numeric(df['T20_id'])#.astype(int)
df['Player_id']=df['Player_id'].astype(int)
df['Name']=df['Name'].astype(str)
df['Country']=df['Country'].astype(str)  

spark_df=sqlContext.createDataFrame(df)
spark_df.coalesce(1).write.mode("append").option("header","true").format("orc").save("s3a://cleaneddataespn/T20demo/")
spark_df.coalesce(1).write.mode("append").option("header","true").format("csv").save("s3a://cleaneddataespn/T20demo/")
spark_df.show()
