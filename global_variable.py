def global_initialization():
    # Global variables for API keys
    global weather_file, aerodatabox_file
    
    weather_file = 'weather_key.txt'
    aerodatabox_file = 'aerodatabox_key.txt'

    
    # Get list of key for weather and aerodatabox API
   
    global lst_api_weather, lst_api_aerodatabox
    
    def read_key(file_name):
        file = open(file_name, 'r')
        lst_key = file.readlines()
        lst_key = [item.strip()  for item in lst_key if (item.strip() != '')]
        return lst_key

    lst_api_weather = read_key(weather_file)
    lst_api_aerodatabox = read_key(aerodatabox_file)
    
    # Global variable for weather API Key
    global API_key
    
    
    API_key = lst_api_weather[0]
    
    
    # Global variable for aerodatabox key
    global aerodatabox_key
    aerodatabox_key = lst_api_aerodatabox[0]
    
    
    # Global variables for top city
    global top_city
    
    top_city = ['Berlin', 'Paris','Rome', 'Frankfurt am Main', 'Madrid', 'Hamburg', 'Munich', 'Milan', 'Barcelona', 'Vienna', 'Prague', 'Warsaw']
    
    
    # Global variables for API weather
    global querystring_airport, querystring_flight
    
    querystring_airport = {"withFlightInfoOnly":"True"}
    querystring_flight = {"withLeg":"false","direction":"Both","withCancelled":"false","withCodeshared":"true","withCargo":"false","withPrivate":"true","withLocation":"false"}

    # Global variables for API flight information

    global headers
    
    headers = {
    "X-RapidAPI-Key": "0c86aab83fmsh048347b7f97485dp1ae8b9jsn2d7ce9c5ddfd",
    "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
    }
    

    # Global variables for connection
    global schema
    
    f = open('sql_pass.txt', 'r')
    sql_pass = f.read()

    schema = "ngoc_gan_v1"
    host = "127.0.0.1"
    user = "root"
    password = sql_pass
    port = 3306

    
    
    global con
    con = f'mysql+pymysql://{user}:{password}@{host}:{port}'
    
    # Global variable for connection with schema
    
    global con_schema
    con_schema = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'

    # Global variables for table names
    global demographic_table, airport_table, city_table, flight_table, weather_table
    
    demographic_table = 'demographic'
    airport_table = 'airport'
    city_table = 'city'
    flight_table = 'flight'
    weather_table = 'weather'
    
    # Global variable for column names
    global city_col, demographic_col, airport_col, weather_col, flight_col
    
    city_col = ['city_id', 'city', 'country', 'longitude', 'latitude', 'link']
    demographic_col = ['city', 'population', 'date_population', 'city_id', 'country']
    airport_col = ['icao', 'iata', 'name', 'short_name', 'city', 'country_code', 'latitude', 'longitude', 'city_id']
    weather_col = ['city_id', 'city', 'time', 'temprerature', 'humidity', 'wind_speed', 'visibility', 'weather_description', 'prob_raining', 'rain_vol_3h', 'snow_vol_3h', 'retrieved_date']
    flight_col = ['arrival_icao', 'departure_ciao', 'departure_name', 'flight_number', 'airline_name', 'scheduled_time', 'city_id']