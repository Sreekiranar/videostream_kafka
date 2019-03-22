from picamera.array import PiRGBArray
from picamera import PiCamera
import time
import cv2
from kafka import KafkaProducer
#Initialising Kafka Producer, cluster IP :<your Kafka server IP>, topic: video
producer = KafkaProducer(bootstrap_servers='<your kafka server IP>:9092')
topic = 'video' #set the topic name
#Initialising PiCamera
camera = PiCamera()
resolution=(1280,720)
camera.resolution = resolution
camera.framerate = 10
rawCapture = PiRGBArray(camera, size=resolution)

time.sleep(0.1)
print "Capturing!!!"

for frame in camera.capture_continuous(rawCapture, format="bgr", use_video_port=True):
	#get image matrix        
	image = frame.array
	#convert image matrix to ndarray
        ret,jpeg = cv2.imencode('.png', image)
	
	#convert ndarray into bytes and sending to kafka server
        producer.send(topic, jpeg.tobytes())
        rawCapture.truncate(0)

