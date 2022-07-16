# Add global variables
import global_variable
global_variable.global_initialization()
from global_variable import *


# Scraping big city in Germany
def big_city_wiki():
    from bs4 import BeautifulSoup
    import requests
    import pandas as pd
    url = 'https://en.wikipedia.org/wiki/List_of_cities_in_Germany_by_population'
    response = requests.get(url)
    response.status_code
    soup = BeautifulSoup(response.content, 'html.parser')
    city_lst = []
    i = 0
    for elem in soup.select('table[class="wikitable sortable"]')[0].select('tr'):
        if (i > 0):
            row = elem.select('td')
            city = row[1].get_text().strip()
            index = city.find('(')
            if (index != -1):
                city = city[:index].strip()
            state = row[2].get_text().strip()
            pop_2015 = row[3].get_text().strip()
            pop_2011 = row[4].get_text().strip()
            area_2015 = row[6].get_text().strip()
            index = area_2015.find('\xa0')
            area_2015 = area_2015[:index]
            density = row[7].get_text().strip()
            index = density.find('/')
            density = density[:index]
            location = row[8].select('.geo-dec')[0].get_text().split()
            latitude = location[0]
            longitude = location[1]
            lst = [city, state, pop_2015, pop_2011, area_2015, density, latitude, longitude]
            city_lst.append(lst)
        else:
            i=i+1
    df_city = pd.DataFrame(city_lst, columns=['city', 'state', 'population_2015', 'population_2011', 'area_2015', 'density', 'latitude', 'longitude'])
    return df_city

# Scraping big cities in Europe

# Function to clean longitude
def clean_longitude(lon):
    direction = lon['longitude'][-1]
    value = float(lon['longitude'][:-2].replace('°', '.').replace('′', ''))
    if (direction == 'W'):
        value = - value
    return value

# Function to clean latitude
def clean_latitude(lat):
    direction = lat['latitude'][-1]
    value = float(lat['latitude'][:-2].replace('°', '.').replace('′', ''))
    if (direction == 'S'):
        value = -value
    return value

# Function to clean dataframe
def clean_dataframe(df):
    df['city'] = df.city.str.strip()
    df['country'] = df.country.str.strip()
    df['longitude'] = df.longitude.str.strip()
    df['latitude'] = df.latitude.str.strip()
    df['link'] = df.link.str.strip()
    
    # Add column city_id as primary key
    df['city_id'] = df.apply(lambda row: row['city'][:3].upper()+'-'+row['country'][:3].upper(), axis =1)
    
    # Change longitude column
    df['longitude'] = df.apply(clean_longitude, axis = 1)
    
    # Change latitude
    df['latitude'] = df.apply(clean_latitude, axis = 1)
    
    # Change the order of column
    cols = df.columns.to_list()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]
    return df

def wiki_city(city_name):
    from bs4 import BeautifulSoup
    import requests
    import pandas as pd
    url = f'https://en.wikipedia.org/wiki/{city_name}'
    response = requests.get(url)
    lst = []
    if (response.status_code != 200):
        print('There is no information from city ', city_name)
        df = pd.DataFrame(columns = ['city', 'country', 'longitude', 'latitude', 'link'])
    else:
        soup = BeautifulSoup(response.content, 'html.parser')
        dict_city = {} 
        dict_city['city'] = city_name
        dict_city['country'] = soup.select('.mergedtoprow > .infobox-data')[0].get_text()
        dict_city['longitude'] = soup.select('.longitude')[0].get_text()        
        dict_city['latitude'] = soup.select('.latitude')[0].get_text()        
        dict_city['link'] = soup.select('.infobox-label:-soup-contains("Website")')[0].parent.a['href']
        lst.append(dict_city)
        df = pd.DataFrame(lst)
    
    return df

def wiki_top_city(top_city):
    import pandas as pd
    lst_df = []
    for city_name in top_city:
        df = wiki_city(city_name)
        lst_df.append(df)
        
    df_city = clean_dataframe(pd.concat(lst_df, ignore_index=True))
    global_variable.city_col = list(df_city.columns)
    return df_city

# Get demographic data
                           
def clean_demographic(df_demographic, df_city):    
    df_demographic['population'] = df_demographic['population'].str.replace(',', '')
    return df_demographic.merge(df_city[['city_id', 'city', 'country']], how = 'right', on = 'city')                           

def get_demographic():                           
    # url = 'https://en.wikipedia.org/wiki/List_of_cities_in_the_European_Union_by_population_within_city_limits'
    url = 'https://en.wikipedia.org/wiki/List_of_cities_in_the_European_Union_by_population_within_city_limits'
    from bs4 import BeautifulSoup
    import requests
    from datetime import datetime
    import pandas as pd
    response = requests.get(url)
    if (response.status_code == 200):
        soup = BeautifulSoup(response.content, 'html.parser')
        tbl = soup.select("table[class = 'wikitable sortable']")[0].select('tr')[1:]
        lst_demographic = []
        for item in tbl:
            city = item.select('td')[1].get_text().strip()
            population = item.select('td')[3].get_text().strip()
            date_population = item.select('td')[4].get_text().strip()
            # date_population = datetime.strptime(date, '%d %B %Y')
            date_population = pd.to_datetime(date_population)
            lst = [city, population, date_population]
            lst_demographic.append(lst)
        df_demographic = pd.DataFrame(lst_demographic, columns=['city', 'population', 'date_population'])
        global_variable.demographic_col = list(df_demographic.columns)
        df_city = pd.read_sql('SELECT * FROM city', global_variable.con_schema)
        df_demographic = clean_demographic(df_demographic, df_city)
        global_variable.demographic_col = df_demographic.columns
    else:
        print('There is no information')
        df_demographic = pd.DataFrame(columns = global_variable.demographic_col)
    return df_demographic

# Get API weather key from the list
def get_api_weather():
    city = 'Berlin'
    api = global_variable.API_key
    try:
        index = global_variable.lst_api_weather.index(api)
    except:
        index = 0
    length = len(global_variable.lst_api_weather)
    for i in range (length):
        api = global_variable.lst_api_weather[(i + index) % length]
        url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api}&units=metric"
        results = requests.get(url)
        if (results.status_code == 200):
            global_variable.API_key = api
            break
    # print(global_variable.API_key)       

# Get Weather data
def get_data_5days_3hours(city):                 
    from bs4 import BeautifulSoup
    import requests
    from datetime import datetime
    import pandas as pd
    import pytz
    
    # add time zone
    tz = pytz.timezone('Europe/Berlin')
    now = datetime.now().astimezone(tz)
    
    # Find API for weather from the list
    api_weather = global_variable.API_key
    try:
        index = global_variable.lst_api_weather.index(api_weather)
    except:
        index = 0
    length = len(global_variable.lst_api_weather)
    for i in range (length):
        api_weather = global_variable.lst_api_weather[(i + index) % length]
        url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_weather}&units=metric"
        results = requests.get(url)
        if (results.status_code == 200):
            global_variable.API_key = api_weather
            break
            
    # url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={global_variable.API_key}&units=metric"
    # results = requests.get(url)
    
    weather_response = results.json()
    lst_weather = []
         
    if (results.status_code == 200):
        for element in weather_response['list']:
            temp = element['main']['temp']
            humidity = element['main']['humidity']
            wind_speed = element['wind']['speed']
            visibility = element['visibility']
            time = element['dt_txt']
            weather_descrip = element['weather'][0]['description']
            if ('pop' not in list(element.keys())):
                pop_rain = 0
                rain_volume_3h = 0
            else:
                pop_rain = element['pop']
                if ('rain' not in list(element.keys())):
                    rain_volume_3h = 0
                else:
                    rain_volume_3h = element['rain']['3h']
            if ('snow' not in list(element.keys())):
                snow_volume_3h = 0
            else:
                snow_volume_3h = element['snow']['3h']
            lst = [city, time, temp, humidity, wind_speed, visibility, weather_descrip, pop_rain, rain_volume_3h, snow_volume_3h, now]
            lst_weather.append(lst)        
    # else:        
    #     pass
    return lst_weather

def add_time_zone(time_str):
    import pytz
    from datetime import datetime
    tz = pytz.timezone('Europe/Berlin')
    return datetime.fromisoformat(time_str).astimezone(tz)

def get_weather_data():
    import pytz
    import pandas as pd
    lst_city = []
    df_city = pd.read_sql('SELECT * FROM city', global_variable.con_schema)
    city_name = list(df_city['city'])
    for i in range(df_city.shape[0]):
        city = city_name[i]        
        lst_weather = get_data_5days_3hours(city)        
        if (len(lst_weather)!=0):
            city_id = list(df_city.loc[df_city.city == city, 'city_id'])
            lst_weather = [city_id + item for item in  lst_weather] # Add city_id column to the table
            lst_city.append(lst_weather)
            
    lst_city = [elem for sub_lst in lst_city for elem in sub_lst]
    df_weather_city = pd.DataFrame(lst_city, columns= global_variable.weather_col) #['city_id', 'city', 'time', 'temperature', 'humidity', 'wind_speed', 'visibility', 'weather_description', 'prob_raining', 'rain_vol_3h', 'snow_vol_3h', 'retrieved_date'])
    
    if (df_weather_city.shape[0] != 0):
        df_weather_city['time'] = df_weather_city['time'].apply(add_time_zone).dt.strftime('%y-%m-%d %H:%M')
        df_weather_city['retrieved_date'] = df_weather_city['retrieved_date'].dt.strftime('%y-%m-%d %H:%M') # %z for timezone

    else:
        print('Could not get weather data')
    # global_variable.weather_col = df_weather_city.columns
    
    return df_weather_city

# Airport information

# Add airport information with city name

def airport_infomation_city(city_name): # airport information with lat, long
    import pandas as pd
    import requests
    lst_df = []
    df_city = pd.read_sql(f'SELECT * FROM city WHERE city.city = "{city_name}"', global_variable.con_schema)
    
    for i in range(df_city.shape[0]):
        lon = df_city.loc[i, 'longitude']
        lat = df_city.loc[i, 'latitude']
        df_airport = airport_infomation_coordinate(lat, lon)
        if (df_airport.shape[0] != 0):
            lst_df.append(df_airport)

    if (len(lst_df) != 0):
        df_airport = pd.concat(lst_df, ignore_index=True)
        # df_airport.columns = ['icao', 'iata', 'name', 'short_name', 'city', 'country_code', 'latitude', 'longitude']
        # df_airport['city'] = df_airport['city'].str.replace('-', ' ')
        df_airport = df_airport[df_airport.city.isin(df_city.city)]
    else:
        df_airport = pd.DataFrame([], columns = global_variable.airport_col[:-1])#['icao', 'iata', 'name', 'short_name', 'city', 'country_code', 'latitude', 'longitude'])
    
    # global_variable.airport_col = df_airport.columns
    df_airport = df_airport.merge(df_city[['city', 'city_id']], on = ['city'])
    if (df_airport.shape[0] == 0):
        print('There is no airport information about the city', city_name)
    return df_airport  

def airport_infomation_coordinate(lat, long): # airport information with lat, long
    import pandas as pd
    import requests
    lst_df = []
    # df_city = pd.read_sql('SELECT * FROM city', global_variable.con_schema)
    
    api_aerodatabox = global_variable.aerodatabox_key
    try:
        index = global_variable.lst_api_aerodatabox.index(api_aerodatabox)
    except:
        index = 0
    length = len(global_variable.lst_api_aerodatabox)
    
    for i in range (length):
        api_aerodatabox = global_variable.lst_api_aerodatabox[(i + index) % length]
        
        url = f"https://aerodatabox.p.rapidapi.com/airports/search/location/{lat}/{long}/km/100/10"

        headers_airport = {"X-RapidAPI-Key": str(api_aerodatabox), "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"}
        response = requests.request("GET", url, headers = headers_airport, params = global_variable.querystring_airport)
        
        if (response.status_code == 200):
            global_variable.aerodatabox_key = api_aerodatabox
            break    
    
    if (response.status_code == 200):
        airport_info = response.json()
        df_airport = pd.json_normalize(airport_info['items'])
        df_airport.columns = global_variable.airport_col[:-1] #['icao', 'iata', 'name', 'short_name', 'city', 'country_code', 'latitude', 'longitude']
        df_airport['city'] = df_airport['city'].str.replace('-', ' ') # Exception Frankfurt-am-Main
        
        # df_airport = df_airport[df_airport.city.isin(df_city.city)]
    else:
        df_airport = pd.DataFrame([], columns = global_variable.airport_col[:-1])# ['icao', 'iata', 'name', 'short_name', 'city', 'country_code', 'latitude', 'longitude'])
    # df_airport = df_airport.merge(df_city[['city', 'city_id']], on = ['city'])
    
    return df_airport
    
    
def airport_infomation():    
    import pandas as pd
    import requests
    
    lst_df = []
    df_city = pd.read_sql('SELECT * FROM city', global_variable.con_schema)
      
            
    for i in range(df_city.shape[0]):
        lon = df_city.loc[i, 'longitude']
        lat = df_city.loc[i, 'latitude']
        df_airport = airport_infomation_coordinate(lat, lon)
        if (df_airport.shape[0] != 0):
            lst_df.append(df_airport)

    if (len(lst_df) != 0):
        df_airport = pd.concat(lst_df, ignore_index=True)
        # df_airport.columns = ['icao', 'iata', 'name', 'short_name', 'city', 'country_code', 'latitude', 'longitude']
        # df_airport['city'] = df_airport['city'].str.replace('-', ' ')
        df_airport = df_airport[df_airport.city.isin(df_city.city)]
    else:
        df_airport = pd.DataFrame([], columns = global_variable.airport_col[:-1])#['icao', 'iata', 'name', 'short_name', 'city', 'country_code', 'latitude', 'longitude'])
    
    # global_variable.airport_col = df_airport.columns
    df_airport = df_airport.merge(df_city[['city', 'city_id']], on = ['city'])
    if (df_airport.shape[0] == 0):
        print('Could not get airport information, check API key')
    return df_airport                        

# Flight information
                           
def get_flight_infomation_with_icao(icao, time_range):
    import pandas as pd
    import requests
    
    api_aerodatabox = global_variable.aerodatabox_key
    try:
        index = global_variable.lst_api_aerodatabox.index(api_aerodatabox)
    except:
        index = 0
    length = len(global_variable.lst_api_aerodatabox)
    
    for i in range (length):
        api_aerodatabox = global_variable.lst_api_aerodatabox[(i + index) % length]
        
        url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao}/{time_range}"

        headers_airport = {"X-RapidAPI-Key": str(api_aerodatabox), "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"}
        response = requests.request("GET", url, headers = headers_airport, params = global_variable.querystring_flight)
        
        if (response.status_code == 200):
            global_variable.aerodatabox_key = api_aerodatabox
            break  
    
    if (response.status_code == 200):
        results = response.json()['arrivals']
        lst_flight_time = []
        for element in results:
            if 'icao' in element['movement']['airport'].keys():
                departure_icao = element['movement']['airport']['icao']
            else:
                departure_icao = 'unknown'
            if 'name' in element['movement']['airport'].keys():
                departure_name = element['movement']['airport']['name']
            else:
                departure_name = 'unknown'
            scheduled_time = element['movement']['scheduledTimeLocal']
            flight_number = element['number']
            airline_name = element['airline']['name']
            lst_flight_time.append([icao, departure_icao, departure_name, flight_number, airline_name, scheduled_time])
        df_flight = pd.DataFrame(lst_flight_time, columns = global_variable.flight_col[:-1]) #['arrival_icao', 'departure_icao', 'departure_name', 'flight_number', 'airline_name','scheduled_time'])
        return df_flight
    else:        
        return pd.DataFrame([], columns= global_variable.flight_col[:-1]) # ['arrival_icao', 'departure_icao', 'departure_name', 'flight_number', 'airline_name','scheduled_time'])
    
# Get all flight infomation tomorrow for specific icao  
def get_flight_information_with_ciao_tomorrow(icao):
    import datetime
    from datetime import timedelta
    import pandas as pd
    tomorrow = datetime.date.today() + timedelta(1)
    time_range_1 = tomorrow.strftime('%Y-%m-%dT00:00/%Y-%m-%dT11:59')
    time_range_2 = tomorrow.strftime('%Y-%m-%dT12:00/%Y-%m-%dT23:59')
    df_1 = get_flight_infomation_with_icao(icao, time_range_1)
    df_2 = get_flight_infomation_with_icao(icao, time_range_2)
    df = pd.concat([df_1, df_2], ignore_index = True)
    if (df.shape[0] == 0):
        print('Could not get flight information for airport icao ', icao)
    return df

# Get all flight information tomorrow
def get_flight_information_tomorrow():
    import pandas as pd
    df_airport = pd.read_sql('SELECT * FROM airport', global_variable.con_schema)
    lst_ciao = list(df_airport['icao'])
    lst_flight_information = []
    for i in range(df_airport.shape[0]):
        lst_flight = get_flight_information_with_ciao_tomorrow(lst_ciao[i])
        lst_flight_information.append(lst_flight)
    df_flight = pd.concat(lst_flight_information)   
    df_flight = df_flight.merge(df_airport[['icao', 'city_id']], left_on = 'arrival_icao', right_on = 'icao')
    df_flight.drop(columns = 'icao', inplace = True)
    return df_flight

def get_flight_information_tomorrow_city(city_name):
    import pandas as pd
    df_airport = pd.read_sql(f'SELECT * FROM airport WHERE city = "{city_name}"', global_variable.con_schema)
    lst_ciao = list(df_airport['icao'])
    lst_flight_information = []
    for i in range(df_airport.shape[0]):
        df_flight_one = get_flight_information_with_ciao_tomorrow(lst_ciao[i])
        if (df_flight_one.shape[0]!=0):
            lst_flight_information.append(df_flight_one)
    if (len(lst_flight_information) != 0):
        df_flight = pd.concat(lst_flight_information)
        df_flight = df_flight.merge(df_airport[['icao', 'city_id']], left_on = 'arrival_icao', right_on = 'icao')
        df_flight.drop(columns = 'icao', inplace = True)
    else:
        print('There is no flight information for ', city_name)
        df_flight = pd.DataFrame([], columns = global_variable.flight_col)
    # global_variable.flight_col = df_flight.columns
    return df_flight



# Database

# Drop schema

def drop_schema(schema = global_variable.schema):
    import sqlalchemy
    from sqlalchemy import create_engine
    # Connect to mysql database
    
    alchemy_con = create_engine(global_variable.con)
    connect_alchemy = alchemy_con.connect() # connect_alchemy.close() if you do not use it
    
    query = f"DROP SCHEMA IF EXISTS {schema}"
    connect_alchemy.execute(query)
    connect_alchemy.close()
    
# Create schema

def create_schema(schema = global_variable.schema):
    import sqlalchemy
    from sqlalchemy import create_engine
    # Connect to mysql database
    
    alchemy_con = create_engine(global_variable.con)
    connect_alchemy = alchemy_con.connect() # connect_alchemy.close() if you do not use it    
 
    query = f"CREATE SCHEMA IF NOT EXISTS {schema}"
    connect_alchemy.execute(query)
    connect_alchemy.close()
                           
                           
# Create table with primary and foreign keys

# alchemy_con = create_engine(con)
# connect_alchemy = alchemy_con.connect() # connect_alchemy.close() if you do not use it
  
def create_table(
                 tbl_name, # name of table
                 dict_col, # columne name and datatype of column in dictionary {'col1': 'datatype1 PRIMARY KEY', 'col2': 'datatype2'}
                 lst_primary, # list of primary key ['id1', 'id2']
                 dict_foreign # dict of foreign key with key and table references {'id1 id2': ['reftable', ['id_ref1', 'id_ref2']]}
                ):
    import sqlalchemy
    from sqlalchemy import create_engine
    # Connect to mysql database
    
    alchemy_con = create_engine(global_variable.con_schema)
    connect_alchemy = alchemy_con.connect() # connect_alchemy.close() if you do not use it
    
    column_str = [col_name + ' ' + col_type + ',' for col_name, col_type in dict_col.items()]
    column_str = ''.join(item for item in column_str)
    column_str = column_str[:-1]
    
    # add primary key
    if (len(lst_primary) !=0):
        primary_str = [item + ',' for item in lst_primary]
        primary_str = ''.join(item for item in primary_str)
        primary_str = primary_str[:-1]
        primary_str = f', PRIMARY KEY ({primary_str})'
    else:
        primary_str=''
        
    # add foreign key
    if (len(dict_foreign)!=0):
        for lst_foreign, ref_table in dict_foreign.items():
            
            # keys of table for foreign key
            foreign_str = [item + ',' for item in lst_foreign.split()]
            foreign_str = ''.join(item for item in foreign_str)
            foreign_str = foreign_str[:-1]
            
            # table name and keys of reference table
            ref_table_name = ref_table[0]
            ref_table_key = ref_table[1]
            ref_table_key = [item + ',' for item in ref_table_key]
            ref_table_key = ''.join(item for item in ref_table_key)
            ref_table_key = ref_table_key[:-1]
            
            foreign_final = f', FOREIGN KEY ({foreign_str}) REFERENCES {ref_table_name}({ref_table_key})'
    else:
        foreign_final = ''
    query = f'''CREATE TABLE IF NOT EXISTS {tbl_name} ({column_str}{primary_str}{foreign_final})'''               
    connect_alchemy.execute(query)
    connect_alchemy.close()
    # return query

# insert dataframe to database
def insert_db_from_dataframe(df, con, table_name):
    df.to_sql(table_name, con, if_exists = 'append', index = False)
    
def insert_db_from_dataframe_row(df, city_table): # add each row in dataframe to the database. Return dataframe which is not added to database
    import sqlalchemy
    from sqlalchemy import create_engine
    import pandas as pd
    alchemy_con = create_engine(global_variable.con_schema)
    connect_alchemy = alchemy_con.connect()
    n_col = df.shape[1]
    lst_except = []
    for index, row in df.iterrows():
        query = f"INSERT INTO {city_table}  VALUES (" + "%s,"*(n_col-1)+"%s)"
        try:
            connect_alchemy.execute(query, tuple(row))        
        except:
            lst_except.append(list(row))
    df_except = pd.DataFrame(lst_except, columns=df.columns)
    return df_except
          