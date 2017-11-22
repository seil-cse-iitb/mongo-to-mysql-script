import datetime

import pymongo as pm
import os.path
import os
import json

import time

import sys
from mysql.connector import (connection)

#two files are created when this file run:- transfered_records_log.json and log

#transfered_records_log.json saves how many records are saved till now into mysql; in case, script running fails it will start after the data that is already inserted (since data is around 10GB per table so it was needed to track no. of records inserted otherwise script would be inserting the whole data if it fails any time)

#log file has some log data to track if any error has come

db_name = "kresit"
limit_records = 1000 #this are the no. of records which will be fetched from mongo at once and will be inserted into mysql, after all this records are inserted then only mysql commit will be called else rollback is called
db_user = 'admin'#mysql
db_pass = 'letmein'#mysql
db_host = '10.129.23.41'#mysql
start_time_stamp = time.mktime(datetime.datetime.strptime("08/09/2017 14:03:21", "%d/%m/%Y %H:%M:%S").timetuple()) #1504859601.0  # records are fetched from mongo db after this time stamp (8/9/2017, 14:03:21)
end_time_stamp = time.mktime(datetime.datetime.strptime("08/09/2017 20:00:00", "%d/%m/%Y %H:%M:%S").timetuple()) #1504881000.0
mongo_host = "10.129.23.41"
schema = {
    "temp_k_sr": ['humidity', 'battery_voltage', 'id', 'TS', 'TS_RECV', 'temperature'],
    "power_k_sr_a": ['V12', 'V1', 'V3', 'PF1', 'VAR3', 'W2', 'VA1', 'TS_RECV', 'FwdVARhR', 'VLN', 'W1', 'PF2', 'A',
                     'FwdVAh', 'V31', 'V23', 'VAR', 'VA2', 'PF3', 'FwdWh', 'VAR1', 'W3', 'F', 'A1', 'PF', 'FwdVARhC',
                     'V2', 'VAR2', 'VA', 'VA3', 'W', 'A2', 'A3', 'TS', 'srl', 'VLL'],
    "power_k_sr_p": ['V12', 'V1', 'V3', 'PF1', 'VAR3', 'W2', 'VA1', 'TS_RECV', 'FwdVARhR', 'VLN', 'W1', 'PF2', 'A',
                     'FwdVAh', 'V31', 'V23', 'VAR', 'VA2', 'PF3', 'FwdWh', 'VAR1', 'W3', 'F', 'A1', 'PF', 'FwdVARhC',
                     'V2', 'VAR2', 'VA', 'VA3', 'W', 'A2', 'A3', 'TS', 'srl', 'VLL'],
    "power_k_yc_a": ['V12', 'V1', 'V3', 'PF1', 'VAR3', 'W2', 'VA1', 'TS_RECV', 'FwdVARhR', 'VLN', 'W1', 'PF2', 'A',
                     'FwdVAh', 'V31', 'V23', 'VAR', 'VA2', 'PF3', 'FwdWh', 'VAR1', 'W3', 'F', 'A1', 'PF', 'FwdVARhC',
                     'V2', 'VAR2', 'VA', 'VA3', 'W', 'A2', 'A3', 'TS', 'srl', 'VLL'],
    "power_k_yc_p": ['V12', 'V1', 'V3', 'PF1', 'VAR3', 'W2', 'VA1', 'TS_RECV', 'FwdVARhR', 'VLN', 'W1', 'PF2', 'A',
                     'FwdVAh', 'V31', 'V23', 'VAR', 'VA2', 'PF3', 'FwdWh', 'VAR1', 'W3', 'F', 'A1', 'PF', 'FwdVARhC',
                     'V2', 'VAR2', 'VA', 'VA3', 'W', 'A2', 'A3', 'TS', 'srl', 'VLL'],
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
        if (table_name in dict.keys())==False:
            dict[table_name]=0
        return dict[table_name]

try:
    mo_db, mo_con = connect_mongo()  # connection to mongo db
except:
    print("Unexpected error In Mongo Connection:", sys.exc_info()[0])
    tFile = str(time.strftime("%d-%m-%Y"))
    tWrite = time.strftime("%H:%M:%S", time.localtime(time.time()))
    with open(
                    "/media/hail-drago/Work/Study/IITB Study/SEIL Lab/Mongo To MySQL Script/mongotomysqlscripterrror/error_" + tFile,
                    "a+") as errF:
        errF.write(str(tWrite) + "\n" + str(sys.exc_info()[0]))

# username:password
# writer:datapool
# admin:letmein

mysqlcon = connection.MySQLConnection(user=db_user, password=db_pass,
                                      host=db_host)
cursor = mysqlcon.cursor()

try:
    cursor.execute("use " + db_name) #if database is not found then it will create it self (*see in except)
except:
    # reset no. of records transferred to 0
    for table_name in schema.keys():
        set_no_of_transfered_records(table_name, 0)
    cursor.execute("create database if not exists " + db_name)
    cursor.execute("use " + db_name)

for table_name in schema.keys(): #creates table if not exists
    cols = schema[table_name]
    query = "Create table if not exists `" + table_name + "` ("
    for col_name in cols:
        query += " `" + col_name + "` double NULL,"
    query = query[:-1]
    query += ")"
    cursor.execute(query)

try:
    for table_name in schema.keys():
        count = 1
        mysqlcon.rollback()
        while count != 0:
            select_cols = get_param_dict(schema[table_name])
            query = {"$query": {"TS": {"$gt": start_time_stamp,"$lt": end_time_stamp }}, "$orderby": {"TS": 1}}
            print(table_name)
            offset = get_no_of_transfered_records(table_name)
            print("offset: " + str(offset))
            rows = mo_con[table_name].find(query, select_cols).skip(offset).limit(limit_records)
            count = 0
            for row in rows:
                query = "insert into `" + table_name + "` "
                col_str = ""
                value_str = ""
                for col in schema[table_name]:
                    if row[col] == None:
                        value = "NULL"
                    else:
                        value = "'" + str(row[col]) + "'"
                    col_str += "`" + col + "`,"
                    value_str += " " + str(value) + ","
                value_str = value_str[:-1]
                col_str = col_str[:-1]
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
