import mysql.connector as mysql
import pandas as pd
import ssl
from mysql.connector import Error
from urllib.request import Request, urlopen
from Utils.load_preferences import get_preferences
from datetime import datetime
ssl._create_default_https_context = ssl._create_unverified_context

#creamos un array con las estaciones y nos quedamos con la parte que nos interesa y es comun a todas las mediciones
stations = ["28079008", "28079011", "28079016", "28079017", "28079018", "28079024", "28079027", "28079035", "28079036", "28079038", "28079039", "28079040", "28079047", "28079048", "28079049",
            "28079050", "28079054", "28079055", "28079056", "28079057", "28079058", "28079059", "28079060", "28005002", "28006004", "28007004", "28009001", "28013002", "28014002", "28016001",
            "28045002", "28047002", "28049003", "28058004", "28065014", "28067001", "28074007", "28080003", "28092005", "28102001", "28120001", "28123002", "28133002", "28148004", "28161001",
            "28171001", "28180001"]

#creamos dos arrays para asignar las latitudes y longitudes a cada estacion
lats = ["40.25", "40.45", "40.27", "40.34", "40.39", "40.25", "40.29", "40.25", "40.40", "40.26", "40.47", "40.38", "40.23", "40.45", "40.41",
        "40.28", "40.36", "40.45", "40.23", "40.29", "40.33", "40.27", "40.50", "40.28", "40.32", "40.20", "40.35", "40.01", "40.18", "40.54",
        "40.39", "40.37", "40.25", "40.16", "40.18", "40.46", "40.20", "40.26", "40.19", "40.17", "40.49", "40.21", "40.22", "40.26", "40.11",
        "40.14", "40.10"]
longs = ["-3.40", "-3.66", "-3.39", "-3.70", "-3.73", "-3.45", "-3.57", "-3.42", "-3.64", "-3.42", "-3.70", "-3.65", "-3.40", "-3.69", "-3.68",
         "-3.41", "-3.60", "-3.63", "-3.43", "-3.39", "-3.46", "-3.36", "-3.69", "-3.22", "-3.38", "-3.50", "-3.30", "-3.35", "-3.27", "-3.28",
         "-3.46", "-4.00", "-3.32", "-3.48", "-3.43", "-3.42", "-3.45", "-3.52", "-3.52", "-3.13", "-3.57", "-3.32", "-4.23", "-3.28", "-3.40",
         "-4.16", "-3.16"]


urls = ["https://www.mambiente.madrid.es/opendata/horario.csv", "https://datos.comunidad.madrid/catalogo/dataset/cb5b856f-71a4-4e34-8539-84a7e994c972/resource/9fd86617-370a-4770-8a92-0c42ea02d6a1/download/calidad_aire_datos_dia.csv"]


def connect_database():
    file = 'db_conf.yaml'
    params = get_preferences(file)

    conn = mysql.connect(
        host=params["dbhost"],
        user=params["dbuser"],
        password=params["dbpassword"],
        database=params["dbdatabase"]
    )

    return conn


def download_from_url(url):
    req = Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0')
    content = urlopen(req)
    data = pd.read_csv(content, index_col=False, delimiter=';')
    data.columns = [x.upper() for x in data.columns]
    return data


def get_last_value_from_file(row):
    value = ""
    for x in range(1, 25):
        if x < 10:
            if row["V0" + str(x)] == "V" or row["V0" + str(x)] == "T":
                value = row["H0" + str(x)]
        elif x > 10:
            if row["V" + str(x)] == "V" or row["V" + str(x)] == "T":
                value = row["H" + str(x)]
    return value


def get_last_hour_from_file(row):
    hour = ""
    for y in range(1, 25):
        if y < 10:
            if row["V0" + str(y)] == "V" or row["V0" + str(y)] == "T":
                hour = str(y)
        if y > 10:
            if row["V" + str(y)] == "V" or row["V" + str(y)] == "T":
                hour = str(y)
    return hour


def web_register(data):
    carga = data.get('fichero', None)
    print(carga)
    if carga == "true" or carga is None or carga == '':
        try:
            for u, val in enumerate(urls):
                conn = connect_database()
                url = urls[u]
                data = download_from_url(url)
                data.head()
                if conn.is_connected():
                    cursor = conn.cursor()
                    cursor.execute("select database();")
                    record = cursor.fetchone()
                    print("You're connected to database: ", record)
                    now = datetime.now()
                    for i, row in data.iterrows():
                        # como en la definicion de requisitos esta definido que solo nos interesan unos gases en concreto, seleccionamos esos gases al leer el campo "magnitud" del dataframe
                        if row["MAGNITUD"] == 6:  # Si es monoxido de carbono, CO
                            for j, val in enumerate(stations):  # recorremos el array de estaciones, para ir encontrando cada estacion
                                if row["PUNTO_MUESTREO"].find(stations[j]) != -1:
                                    print(row)
                                    sql = "INSERT INTO tfgdata2 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"  # sentencia SQL de insercion
                                    date = str(row[7]) + '/' + str(row[6]) + '/' + str(row[5])  # concatenamos los campos que de las fechas (anyo, mes, dia)
                                    data = (lats[j], longs[j], date, get_last_hour_from_file(row), "CO", get_last_value_from_file(row), "static", now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "web")
                                    cursor.execute(sql, data)
                                    print("Record inserted")
                                    conn.commit()
                        if row["MAGNITUD"] == 8:  # si es dioxido de nitrogeno, NO2
                            for j, val in enumerate(stations):  # recorremos el array de estaciones, para ir encontrando cada estacion
                                if row["PUNTO_MUESTREO"].find(stations[j]) != -1:
                                    sql = "INSERT INTO tfgdata2 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"  # sentencia SQL de insercion
                                    date = str(row[7]) + '/' + str(row[6]) + '/' + str(row[5])  # concatenamos los campos que de las fechas (anyo, mes, dia)
                                    data = (lats[j], longs[j], date, get_last_hour_from_file(row), "NO2", get_last_value_from_file(row), "static", now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "web")
                                    cursor.execute(sql, data)  # ejecutamos la sentencia SQL con los datos que queremos
                                    print("Record inserted")
                                    conn.commit()
                        elif row["MAGNITUD"] == 14:  # si es ozono, O3
                            for j, val in enumerate(stations):  # recorremos el array de estaciones, para ir encontrando cada estacion
                                if row["PUNTO_MUESTREO"].find(stations[j]) != -1:
                                    sql = "INSERT INTO tfgdata2 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"  # sentencia SQL de insercion
                                    date = str(row[7]) + '/' + str(row[6]) + '/' + str(row[5])  # concatenamos los campos que de las fechas (anyo, mes, dia)
                                    data = (lats[j], longs[j], date, get_last_hour_from_file(row), "O3", get_last_value_from_file(row), "static", now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "web")
                                    cursor.execute(sql, data)  # ejecutamos la sentencia SQL con los datos que queremos
                                    print("Record inserted")
                                    conn.commit()
                        elif row["MAGNITUD"] == 1:  # si es dioxido de azufre, SO2
                            for j, val in enumerate(stations):  # recorremos el array de estaciones, para ir encontrando cada estacion
                                if row["PUNTO_MUESTREO"].find(stations[j]) != -1:
                                    sql = "INSERT INTO tfgdata2 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"  # sentencia SQL de insercion
                                    date = str(row[7]) + '/' + str(row[6]) + '/' + str(row[5])  # concatenamos los campos que de las fechas (anyo, mes, dia)
                                    data = (lats[j], longs[j], date, get_last_hour_from_file(row), "SO2", get_last_value_from_file(row), "static", now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "web")
                                    cursor.execute(sql, data)  # ejecutamos la sentencia SQL con los datos que queremos
                                    print("Record inserted")
                                    conn.commit()
            return True
        except Error as e:
            print("Error while connecting to MySQL", e)
            return False
    else:
        try:
            conn = connect_database()
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                print("You're connected to database: ", record)
                now = datetime.now()
                sql = "INSERT INTO tfgdata2 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"  # sentencia SQL de insercion
                data = (data['latitud'], data['longitud'], data['date'], data['hour'], data['gas'], data['gas_value'], data['station_type'],now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], data['origin_measure'])
                cursor.execute(sql, data)  # ejecutamos la sentencia SQL con los datos que queremos
                print("Record inserted")
                conn.commit()
                return True
            return False
        except Error as e:
            print("Error while connecting to MySQL", e)
            return False