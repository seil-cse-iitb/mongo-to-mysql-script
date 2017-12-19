import json

import requests

response= requests.get("http://10.129.23.41:8080/meta/sensors/")
sensors = response.json()
tables ={}
count=0
for channel in [1,3,5,7]:
    table = []
    for sensor in sensors:
        if(sensor['channel'] == channel):
            table.append(str(sensor['sensor_id']))
            count+=1
    tables[str(channel)]=table

print(json.dumps(tables))
file = open("channelwise-tables.json",'w')
json.dump(tables,file)
print(count)