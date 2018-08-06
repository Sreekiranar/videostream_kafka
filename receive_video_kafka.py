from kafka import KafkaConsumer
import cv2
import numpy as np
from signal import signal, SIGPIPE, SIG_DFL

#Initialising kafka consumer from kafka server IP:<Your Kafka IP> and topic : video
topic='video'
consumer = KafkaConsumer(topic,bootstrap_servers =['<Your Kafka IP>:9092'])

#this is to seek the consumer to the last message, Thus, When you start consumer it starts with the real-time data only.
try: 
    consumer.poll()
    consumer.seek_to_end() 
except:
    print "latest"
#By default, the consumer starts from wherever you left off. If you want it like that, comment the above chunk. 

#reading the messages
for msg in consumer:
    #Converting the obtained bytes to ndarray
    nparr = np.fromstring(msg.value, np.uint8)
    flags = cv2.IMREAD_COLOR
    #converting ndarray to image matrix
    frame = cv2.imdecode(nparr,flags)
    #showing the output 
    cv2.imshow('Raspberry Pi Video',frame)
    if cv2.waitKey(1) & 0xFF == ord('q'):
    	break	
#When everything is over, destroying the windows
cv2.destroyAllWindows()
#controlling the flow
signal(SIGPIPE,SIG_DFL) 

