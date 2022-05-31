import mysql.connector as mysql
import random
from mysql.connector import Error
from Utils.load_preferences import get_preferences
from datetime import datetime


gas = ["CO", "NO2", "O3", "SO2"]
stations = ["static", "dynamic"]


def random_lat():
    lat = str(round(random.uniform(40.11, 41.13), 2))
    return lat


def random_long():
    long = str(round(random.uniform(-4.23, -3.15), 2))
    return long


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


def get_comma_pos(str):
    for pos, a in enumerate(str):
        if a == ".":
            return pos


def simulate_measure_station(gasname):
    aux_conn = connect_database()
    aux = str(gasname)
    if aux_conn.is_connected():
        cursor_aux = aux_conn.cursor()
        sql_aux = "SELECT AVG(gas_measure) AS average FROM tfgdata2 WHERE gas_name = %s"
        cursor_aux.execute(sql_aux, (aux,))
        rows = cursor_aux.fetchall()
    for i in rows:
        average = str(i[0])
    average = average[0:get_comma_pos(average)+2]
    #average = average[0:3]
    aux_conn.commit()
    aux_conn.close()
    return average


def retrieve_last_date_measurement():
    aux = connect_database()
    result = ""
    if aux.is_connected():
        cursor_aux = aux.cursor()
        sql_aux = "SELECT date_measure FROM tfgdata2 ORDER BY date_measure DESC LIMIT 1"
        cursor_aux.execute(sql_aux)
        result = cursor_aux.fetchone()[0]
    result = str(result)
    aux.commit()
    aux.close()
    return result


def retrieve_last_hour_measurement():
    aux2 = connect_database()
    result = ""
    if aux2.is_connected():
        cursor_aux = aux2.cursor()
        sql_aux = "SELECT hour_measure FROM tfgdata2 ORDER BY hour_measure DESC LIMIT 1"
        cursor_aux.execute(sql_aux)
        result = cursor_aux.fetchone()[0]
    result = str(result)
    aux2.commit()
    aux2.close()
    return result


def simulation_register():
    sql = "INSERT INTO tfgdata2 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"  # sentencia SQL de insercion
    latitude = random_lat()
    longitude = random_long()
    try:
        for u, row in enumerate(stations):
            conn = connect_database()
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                print("You're connected to database: ", record)
                now = datetime.now()
                for i, val in enumerate(gas):
                    print(gas[i])
                    # como en la definicion de requisitos esta definido que solo nos interesan unos gases en concreto, seleccionamos esos gases al leer el campo "magnitud" del dataframe
                    if val == "CO":
                        if stations[u] == "static":
                            data = ("40.18", "-3.71", retrieve_last_date_measurement(), retrieve_last_hour_measurement(), "CO", simulate_measure_station(val), row, now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "simulation")
                            cursor.execute(sql, data)
                            print("Record inserted")
                            conn.commit()
                        else:
                            data = (latitude, longitude, retrieve_last_date_measurement(), retrieve_last_hour_measurement(), "CO", simulate_measure_station(val), row, now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "simulation")
                            cursor.execute(sql, data)
                            print("Record inserted")
                            conn.commit()
                    elif val == "NO2":
                        if stations[u] == "static":
                            data = ("40.18", "-3.71", retrieve_last_date_measurement(), retrieve_last_hour_measurement(), "NO2", simulate_measure_station(val), row, now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "simulation")
                            cursor.execute(sql, data)
                            print("Record inserted")
                            conn.commit()
                        else:
                            data = (latitude, longitude, retrieve_last_date_measurement(), retrieve_last_hour_measurement(), "NO2", simulate_measure_station(val), row, now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "simulation")
                            cursor.execute(sql, data)
                            print("Record inserted")
                            conn.commit()
                    elif val == "O3":
                        if stations[u] == "static":
                            data = ("40.18", "-3.71", retrieve_last_date_measurement(), retrieve_last_hour_measurement(), "O3", simulate_measure_station(val), row, now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "simulation")
                            cursor.execute(sql, data)
                            print("Record inserted")
                            conn.commit()
                        else:
                            data = (latitude, longitude, retrieve_last_date_measurement(), retrieve_last_hour_measurement(), "O3", simulate_measure_station(val), row, now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "simulation")
                            cursor.execute(sql, data)
                            print("Record inserted")
                            conn.commit()
                    elif val == "SO2":
                        if stations[u] == "static":
                            data = ("40.18", "-3.71", retrieve_last_date_measurement(), retrieve_last_hour_measurement(), "SO2", simulate_measure_station(val), row, now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "simulation")
                            cursor.execute(sql, data)
                            print("Record inserted")
                            conn.commit()
                        else:
                            data = (latitude, longitude, retrieve_last_date_measurement(), retrieve_last_hour_measurement(), "SO2", simulate_measure_station(val), row, now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], "simulation")
                            cursor.execute(sql, data)
                            print("Record inserted")
                            conn.commit()
        return True
    except Error as e:
        print("Error while connecting to MySQL", e)
        return False