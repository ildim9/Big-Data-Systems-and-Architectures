from kafka import * # importing all the kafka library 
import json
from datetime import datetime as dt 
# Creation of  a KafkaClient Class
admin_client = KafkaAdminClient(bootstrap_servers ="localhost:9092",client_id='task2')

#Defing the empty topic list
topic_list=[]
#Use the NewTopic class to create a topic (“clima”) & determine partition nums & replication factor.
new_topic = NewTopic(name='clima',num_partitions=2,replication_factor=1)
topic_list.append(new_topic)
#Use the create_topics() method to create new topics
admin_client.create_topics(new_topics=topic_list)

#Start producing messages
#We will use KafkaProducer class for this (messages in JSON):
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Creation of the 5 messages.

producer.send('clima',{'temp' : 17,'hum' : 55,'timestamp': dt.now()})

producer.send('clima',{'temp' : 19,'hum' : 58,'timestamp': dt.now()})

producer.send('clima',{'temp' : 12,'hum' : 59,'timestamp': dt.now()})

producer.send('clima',{'temp' : 10,'hum' : 45,'timestamp': dt.now()})

producer.send('clima',{'temp' : 20,'hum' : 65,'timestamp': dt.now()})

# Starting consuming messages

consumer = KafkaConsumer('clima')

#Printing the Messages 

for i in consumer : 
    print(i.value.decode("utf-8"))