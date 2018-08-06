import time
import cv2
from kafka import KafkaProducer
#Initialising Kafka Producer
producer = KafkaProducer(bootstrap_servers='<your Kafka IP>:9092')
topic = 'video'

def video_emitter(video):
    video = cv2.VideoCapture(video)
    print('capturing.....')
    try:
    #get image matrix
        while True:
            success, image = video.read()
            if success == True:   
                #convert image matrix to ndarray
                ret,jpeg = cv2.imencode('.png', image) 
                #convert ndarray into bytes and sending to kafka server
                producer.send(topic, jpeg.tobytes())
                time.sleep(.05)
            else:
                print ("No device Found")
                break
    except KeyboardInterrupt:
        print ("stopped!")
    video.release()
    print ("done")

if __name__ == '__main__':
    video_emitter(0) #change 0 to '<videofilename>.mp4' if you want to stream a video file. 
