import datetime

import pymongo as pm
import os.path
import os
import json

import time

import sys
from mysql.connector import (connection)

# two files are created when this file run:- transfered_records_log.json and log

# transfered_records_log.json saves how many records are saved till now into mysql; in case, script running fails it will start after the data that is already inserted (since data is around 10GB per table so it was needed to track no. of records inserted otherwise script would be inserting the whole data if it fails any time)

# log file has some log data to track if any error has come

limit_records = 1000  # this are the no. of records which will be fetched from mongo at once and will be inserted into mysql, after all this records are inserted then only mysql commit will be called else rollback is called
mongo_host = "10.129.23.41"
db_user = 'root'  # mysql
db_pass = 'MySQL@seil'  # mysql
db_host = '10.129.23.103'  # mysql
db_name = "seil_sensor_data_test"  # mysql
mysql_tables = {
    "1": "rish_1",
    "3": "sch_3",
    "5": "temp_5",
    "7": "dht_7"
}
start_time_stamp = time.mktime(datetime.datetime.strptime("13/12/2017 14:03:21",
                                                          "%d/%m/%Y %H:%M:%S").timetuple())  # 1504859601.0  # records are fetched from mongo db after this time stamp (8/9/2017, 14:03:21)
end_time_stamp = time.mktime(
    datetime.datetime.strptime("13/12/2017 20:00:00", "%d/%m/%Y %H:%M:%S").timetuple())  # 1504881000.0
schema = {
    "1": ['TS_RECV', 'srl', 'TS', 'V1', 'V2', 'V3', 'A1', 'A2', 'A3', 'W1', 'W2', 'W3', 'VA1', 'VA2', 'VA3', 'VAR1', 'VAR2', 'VAR3', 'PF1', 'PF2', 'PF3', 'Ang1', 'Ang2', 'PF3r', 'Ang1r', 'Ang2r', 'Ang3', 'AvgV', 'SumV', 'AvgA', 'SumA', 'AvgW', 'SumW', 'AvgVA', 'SumVA', 'AvgVAr', 'SumVAr', 'AvgPF', 'SumPF', 'AvgAng', 'SumAng', 'F', 'FwdWh'],
    "3": ['TS_RECV', 'srl', 'TS', 'VA', 'W', 'VAR', 'PF', 'VLL', 'VLN', 'A', 'F', 'VA1', 'W1', 'VAR1', 'PF1', 'V12', 'V1', 'A1', 'VA2', 'W2', 'VAR2', 'PF2', 'V23', 'V2', 'A2', 'VA3', 'W3', 'VAR3', 'PF3', 'V31', 'V3', 'A3', 'FwdVAh', 'FwdWh', 'FwdVARhR', 'FwdVARhC'],
    "5": ['TS_RECV', 'TS', 'Zone', 'Lane', 'room', 'temperature'],
    "7": ['TS_RECV', 'TS', 'id', 'temperature', 'humidity', 'battery_voltage'],
}


def connect_mongo():
    db_mo = pm.MongoClient(mongo_host, 27017)
    con = db_mo['data']  # new database
    return db_mo, con


def get_param_dict(params):
    tempd = {}
    for param in params:
        tempd[param] = True
    tempd['_id'] = False
    return tempd


def set_no_of_transfered_records(table_name, no_of_records):
    file_name = "transfered_records_log.json"
    dict = {table_name: no_of_records}
    if (os.path.exists(file_name) == False):
        file = open(file_name, 'w')
        json.dump(dict, file)
    else:
        file = open(file_name, 'r')
        dict = json.load(file)
        file = open(file_name, 'w')
        dict[table_name] = no_of_records
        json.dump(dict, file)
    file.flush()
    print(str(no_of_records) + " Record inserted")
    log = open('log', 'a')
    log.write(table_name + ": " + str(no_of_records) + " Record inserted\n")
    log.close()


def get_no_of_transfered_records(table_name):
    file_name = "transfered_records_log.json"
    dict = {table_name: 0}
    if (os.path.exists(file_name) == False):
        return dict[table_name]
    else:
        file = open(file_name, 'r')
        dict = json.load(file)
        if (table_name in dict.keys()) == False:
            dict[table_name] = 0
        return dict[table_name]


try:
    mo_db, mo_con = connect_mongo()  # connection to mongo db
except:
    print("Unexpected error In Mongo Connection:", sys.exc_info()[0])
    tFile = str(time.strftime("%d-%m-%Y"))
    tWrite = time.strftime("%H:%M:%S", time.localtime(time.time()))
    with open("./mongotomysqlscripterrror/error_" + tFile,
              "a+") as errF:
        errF.write(str(tWrite) + "\n" + str(sys.exc_info()[0]))

mysqlcon = connection.MySQLConnection(user=db_user, password=db_pass,
                                      host=db_host, db=db_name)
cursor = mysqlcon.cursor()

file = open("channelwise-tables.json")
# channelwise_tables = json.load(file)
channelwise_tables = {"1":['aravali_236']}
try:
    for channel in channelwise_tables:
        tables = channelwise_tables[channel]
        for table_name in tables:
            count = 1
            mysqlcon.rollback()
            while count != 0:
                select_cols = get_param_dict(schema[channel])
                query = {"$query": {"TS": {"$gt": start_time_stamp, "$lt": end_time_stamp}}, "$orderby": {"TS": 1}}
                print(channel+": "+table_name +" to "+mysql_tables[channel])
                offset = get_no_of_transfered_records(table_name)
                print("offset: " + str(offset))
                rows = mo_con[table_name].find(query, select_cols).skip(offset).limit(limit_records)
                count = 0
                for row in rows:
                    query = "insert into `" + str(mysql_tables[channel]) + "` "
                    col_str = ""
                    value_str = ""
                    for col in schema[channel]:
                        if row[col] == None:
                            value = "NULL"
                        else:
                            value = "'" + str(row[col]) + "'"
                        col_str += "`" + col + "`,"
                        value_str += " " + str(value) + ","
                    value_str += "'" + table_name + "'"
                    # value_str = value_str[:-1]
                    col_str += "`sensor_id`"
                    # col_str = col_str[:-1]
                    query += "(" + col_str + ") values(" + value_str + ")"
                    try:
                        cursor.execute(query)
                        count += 1
                    except Exception as e:
                        print(e)
                        log = open('log', 'a')
                        log.write(str(e) + "\n")
                        log.close()
                        mysqlcon.rollback()
                        break
                mysqlcon.commit()
                set_no_of_transfered_records(table_name, offset + count)
except Exception as e:
    print(e)
    log = open('log', 'a')
    log.write(str(e) + "\n")
    log.close()
finally:
    mysqlcon.close()
