import json

import pymongo as pm
from mysql.connector import (connection)

mongo_host = "10.129.23.41"
db_user = 'root'#mysql
db_pass = 'MySQL@seil'#mysql
db_host = '10.129.23.42'#mysql

mysql_tables = {
    "1":"rish_1",
    "3":"sch_3",
    "5":"temp_5",
    "7":"dht_7"
}

def get_param_dict(params):
    tempd = {}
    for param in params:
        tempd[param] = True
    tempd['_id'] = False
    return tempd

def connect_mongo():
    db_mo = pm.MongoClient(mongo_host, 27017)
    con = db_mo['data']  # new database
    return db_mo, con
ok=0
def check_table_schema(channel,collection):
    global ok
    try:
        mo_db, mo_con = connect_mongo()  # connection to mongo db
        mysqlcon = connection.MySQLConnection(user=db_user, password=db_pass,
                                              host=db_host,db="seil_sensor_data")
        cursor = mysqlcon.cursor()
    except:
        print("Error")
    cursor.execute("select * from `"+mysql_tables[channel]+"` limit 1")
    mysql_cn = list(cursor.column_names)
    mysql_cn.remove("sensor_id")
    record =mo_con[collection].find_one()
    if record==None:
        print("No record Found:-----"+channel+": "+collection)
        return
    mongo_column_names = list(record.keys())
    mongo_column_names.remove("_id")
    cmp_len = len(set(mysql_cn).intersection(set(mongo_column_names)))
    if(len(mysql_cn)== cmp_len and len(mongo_column_names) == cmp_len):
        print("OK "+channel+": "+collection)
    else:
        print("Not OK------------------"+channel+": "+collection)
        print(mysql_cn)
        print(mongo_column_names)
    ok+=1

file= open("channelwise-tables.json")
channelwise_tables= json.load(file)
count = 0
for channel in channelwise_tables:
    tables  = channelwise_tables[channel]
    for table in tables:
        check_table_schema(channel,table)
print(ok)