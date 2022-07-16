import requests
import pymysql
import json
import sqlalchemy
from sqlalchemy import create_engine
import test
import pandas as pd

from ultility import *
import global_variable
import database_init
import os

def lambda_handler(event, context):
    # TODO implement
    
    schema = "ngoc_gan_v1"
    host = os.environ['host']
    user = "admin"
    password = os.environ['pass_sql_con']
    port = 3306
    

    global_variable.global_initialization()
    database_init.initialization()
    
    df_test = pd.read_sql('select * from city', con_schema)
   
    if (df_test.shape[0] == 0): # check there is available data for city, demographic and airport
        
        df_city = wiki_top_city(global_variable.top_city)
        df_except_city = insert_db_from_dataframe_row(df_city, global_variable.city_table)
        
        df_demographic = get_demographic()
        df_except_demographic = insert_db_from_dataframe_row(df_demographic, global_variable.demographic_table)
        
        df_airport = airport_infomation()
        df_except_airport = insert_db_from_dataframe_row(df_airport, global_variable.airport_table)
        
        df_weather = get_weather_data()
        df_except_weather = insert_db_from_dataframe_row(df_weather, global_variable.weather_table)
        
        df_flight = get_flight_information_tomorrow()
        df_except_flight = insert_db_from_dataframe_row(df_flight, global_variable.flight_table)
        
    else:
        
        df_weather = get_weather_data()
        df_except_weather = insert_db_from_dataframe_row(df_weather, global_variable.weather_table)
        
        df_flight = get_flight_information_tomorrow()
        df_except_flight = insert_db_from_dataframe_row(df_flight, global_variable.flight_table)
    