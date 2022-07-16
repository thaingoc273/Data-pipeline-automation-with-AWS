from ultility import *
import pandas as pd
import global_variable

def initialization():
    global_variable.global_initialization()
    
    # Create schema
    create_schema()
    
    # Create city table
    city_table = global_variable.city_table
    city_column = global_variable.city_col
    city_dict_column = {city_column[0]: 'VARCHAR(10) PRIMARY KEY', # city_id
                        city_column[1]: 'VARCHAR(50) NOT NULL', # city
                        city_column[2]: 'VARCHAR(50) NOT NULL', # country
                        city_column[3]: 'FLOAT(10, 6) NOT NULL', # longitude
                        city_column[4]: 'FLOAT(10, 6) NOT NULL', # latitude
                        city_column[5]: 'TEXT' # link
                       }
    lst_city_primary = []
    dic_city_foreign = []

    create_table(
                 city_table, # name of table
                 city_dict_column, # columne name and datatype of column in dictionary {'col1': 'datatype1 PRIMARY KEY', 'col2': 'datatype2'}
                 lst_city_primary, # list of primary key ['id1', 'id2']
                 dic_city_foreign # dict of foreign key with key and table references {'id1 id2': ['reftable', ['id_ref1', 'id_ref2']]}
                )
    
    # Create airport table
    airport_table = global_variable.airport_table
    airport_column = global_variable.airport_col
    airport_dict_column = {airport_column[0]: 'VARCHAR(10) NOT NULL', # icao
                           airport_column[1]: 'VARCHAR(10)', # iata
                           airport_column[2]: 'VARCHAR(100) NOT NULL', # name
                           airport_column[3]: 'VARCHAR(100)', # shortname
                           airport_column[4]: 'VARCHAR(50)', # city
                           airport_column[5]: 'VARCHAR(20)', # country_code
                           airport_column[6]: 'FLOAT(10, 6) NOT NULL', # latitude
                           airport_column[7]: 'FLOAT(10, 6) NOT NULL', # longitute
                           airport_column[8]: 'VARCHAR(10) NOT NULL' # city_id                       
                          }
    lst_airport_primary = [airport_column[0]]
    lst_airport_foreign = {airport_column[8]: [city_table, [city_column[0]]]}

    create_table(
                 airport_table, # name of table
                 airport_dict_column, # columne name and datatype of column in dictionary {'col1': 'datatype1 PRIMARY KEY', 'col2': 'datatype2'}
                 lst_airport_primary, # list of primary key ['id1', 'id2']
                 lst_airport_foreign # dict of foreign key with key and table references {'id1 id2': ['reftable', ['id_ref1', 'id_ref2']]}
                )
    
    # Create demographic table
    demographic_table = global_variable.demographic_table
    demographic_column = global_variable.demographic_col
    demographic_dict_column = {demographic_column[0]: 'VARCHAR(50) NOT NULL', # city
                               demographic_column[1]: 'BIGINT NOT NULL', # population
                               demographic_column[2]: 'DATETIME NOT NULL', # date_population
                               demographic_column[3]: 'VARCHAR(10) NOT NULL', # city_id
                               demographic_column[4]: 'VARCHAR(50) NOT NULL' # country
                              }
    lst_demographic_primary = [demographic_column[3], demographic_column[2]]
    lst_demographic_foreign = {demographic_column[3]: [city_table, [city_column[0]]]}

    create_table(
                 demographic_table, # name of table
                 demographic_dict_column, # columne name and datatype of column in dictionary {'col1': 'datatype1 PRIMARY KEY', 'col2': 'datatype2'}
                 lst_demographic_primary, # list of primary key ['id1', 'id2']
                 lst_demographic_foreign # dict of foreign key with key and table references {'id1 id2': ['reftable', ['id_ref1', 'id_ref2']]}
                )
    
    
    # Create flight table
    flight_table = global_variable.flight_table
    flight_column = global_variable.flight_col
    flight_dict_column = {flight_column[0]: 'VARCHAR(10)', # arrival_icao
                          flight_column[1]: 'VARCHAR(10)', # departure_icao
                          flight_column[2]: 'VARCHAR(50)', # departure_name
                          flight_column[3]: 'VARCHAR(20) NOT NULL', # flight_number
                          flight_column[4]: 'TEXT', # airline_name
                          flight_column[5]: 'VARCHAR(80) NOT NULL', # scheduled_time
                          flight_column[6]: 'VARCHAR(10) NOT NULL' # city_id
                         }
    lst_flight_primary = [flight_column[0], flight_column[1], flight_column[3], flight_column[5]]
    lst_flight_foreign = {flight_column[0]: [airport_table, [airport_column[0]]]}

    create_table(
                 flight_table, # name of table
                 flight_dict_column, # columne name and datatype of column in dictionary {'col1': 'datatype1 PRIMARY KEY', 'col2': 'datatype2'}
                 lst_flight_primary, # list of primary key ['id1', 'id2']
                 lst_flight_foreign # dict of foreign key with key and table references {'id1 id2': ['reftable', ['id_ref1', 'id_ref2']]}
                )
    
    # Create weather table
    weather_table = global_variable.weather_table
    weather_column = global_variable.weather_col
    weather_dict_col = {weather_column[0]: 'VARCHAR(10) NOT NULL', # city_id
                        weather_column[1]: 'VARCHAR(50) NOT NULL', # city
                        weather_column[2]: 'TIMESTAMP NOT NULL', # time
                        weather_column[3]: 'DECIMAL(6,2)', # temperature
                        weather_column[4]: 'DECIMAL(6,2)', # humidity
                        weather_column[5]: 'DECIMAL(6,2)', # wind_speed
                        weather_column[6]: 'INT', # visibility
                        weather_column[7]: 'VARCHAR(100)', # weather_description
                        weather_column[8]: 'DECIMAL(4,1)', # prob_raining
                        weather_column[9]: 'DECIMAL(6,2)', # rain_vol_3h
                        weather_column[10]: 'DECIMAL(6,2)', # snow_vol_3h
                        weather_column[11]: 'DATETIME NOT NULL' # retrieved_date
                       }
    lst_weather_primary = [weather_column[0], weather_column[2], weather_column[11]]
    lst_weather_foreign = {weather_column[0]: [city_table, [city_column[0]]]}

    create_table(
                 weather_table, # name of table
                 weather_dict_col, # columne name and datatype of column in dictionary {'col1': 'datatype1 PRIMARY KEY', 'col2': 'datatype2'}
                 lst_weather_primary, # list of primary key ['id1', 'id2']
                 lst_weather_foreign # dict of foreign key with key and table references {'id1 id2': ['reftable', ['id_ref1', 'id_ref2']]}
                )