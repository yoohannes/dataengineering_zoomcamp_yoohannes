#!/usr/bin/env python
# coding: utf-8



from unicodedata import name
import pandas as pd
from sqlalchemy import create_engine
from time import time
import os 
import argparse



#df1=pd.read_csv('yellow_tripdata_2021-01.csv',nrows=100)

def main(params):
    user=params.user
    password=params.password
    host=params.host
    port=params.port
    url=params.url
    db=params.db
    table_name=params.table_name
    csv_name='output.csv'
    os.system(f"wget {url} -O {csv_name}")

    emgine=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    emgine.connect()
    #print(pd.io.sql.get_schema(df1,name='yellow_taxi_data',con=emgine))
    df_iter=pd.read_csv(csv_name,iterator=True,chunksize=100000)
    df1=next(df_iter)
    df1.tpep_pickup_datetime=pd.to_datetime(df1.tpep_pickup_datetime)
    df1.tpep_dropoff_datetime=pd.to_datetime(df1.tpep_dropoff_datetime)

    df1.head(0).to_sql(name=table_name,con=emgine,if_exists='append')

    df1.to_sql(name=table_name,con=emgine,if_exists='append')

    while True:
        t_start=time()
        df1=next(df_iter)
        df1.tpep_pickup_datetime=pd.to_datetime(df1.tpep_pickup_datetime)
        df1.tpep_dropoff_datetime=pd.to_datetime(df1.tpep_dropoff_datetime)
        df1.to_sql(name=table_name,con=emgine,if_exists='append')
        t_end=time()
        print('insert another chunk ,took %.3f second' % (t_end-t_start))
if __name__=='__main__':
    parser = argparse.ArgumentParser(description='injest csv data to postgres')

    #user, password, 
    parser.add_argument('--user',help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port  for postgres')
    parser.add_argument('--db', help='database for postgres')
    parser.add_argument('--table_name', help='name of the table where we want to write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    main(args)