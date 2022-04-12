import time
import os
import uuid
import datetime
import random
import json
from kafka import KafkaProducer 
from time import sleep
from json import dumps
from datetime import date, datetime,timedelta
import sys

# These are the vechiles (10 unique vehicle id's)
devices = []
for x in range(0, 10):
    devices.append(str(uuid.uuid4()))

# Create a producer client to produce and publish events to the Kafka.
def serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers = ['wn0-kafkac.fxgwbsxntcxu3byhyrnr5ov3pb.bx.internal.cloudapp.net:9092','wn1-kafkac.fxgwbsxntcxu3byhyrnr5ov3pb.bx.internal.cloudapp.net:9092'], value_serializer=serializer)


# Kafka Topic Name = vehicletelemtry
topicname='vehicletelemtry'
# NYC Borough lat and long
latlong =[[40.730610,-73.984016],[40.650002, -73.949997],
[40.579021, -74.151535],[40.837048, -73.865433],[40.742054, -73.769417]]
lfi=0
y=10
while True:    
    if(y%2==0):
        lfi=random.randint(0,1)
        y=random.randint(1,20)

    latlonglocal = random.choice(latlong)
    try:
        # Creating vehicle reading.
        devid=random.choice(devices)
        #reading = {'vehicle':{'id': devid, 'timestamp': str(datetime.now()), 'other':{'rpm': random.randrange(100), 'speed': random.randint(70, 100)}, 'kms': random.randint(100, 1000)}}
        reading = {'vehicle':{'id': devid, 'eventtime': str(datetime.utcnow()+timedelta(hours=y)), 'rpm': random.randrange(100), 'speed': random.randint(70, 120), 'kms': random.randint(100, 10000),'lfi': lfi,'location':{'lat':latlonglocal[0],'long':latlonglocal[1]}}}
        producer.send(topicname,reading)
        producer.flush()
        print(f"pushing vehicle id  ={devid} to topic - {topicname} at {datetime.now()}")
        time.sleep(3)

    except BufferError as e:
            sys.stderr.write('error sending to kafka!!!')
            producer.close()

# # Close the producer.    