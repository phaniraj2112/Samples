from kafka import KafkaConsumer
import json

topicname="vehicles1"
consumer = KafkaConsumer(bootstrap_servers=['wn0-kafkac.fxgwbsxntcxu3byhyrnr5ov3pb.bx.internal.cloudapp.net:9092','wn1-kafkac.fxgwbsxntcxu3byhyrnr5ov3pb.bx.internal.cloudapp.net:9092'],auto_offset_reset='earliest',api_version=(0, 10, 1))
consumer.subscribe([topicname])
for msg in consumer:
    print (json.loads(msg.value))
