# README #

This repository contains code to stream video stream from Raspberry Pi camera or webcam/ Laptop cameras to Kafka and receive it in any system in the local network and apply algorithms

### How do I get set up? ###

* Components required:
	Raspberry pi 3 with camera/ laptop with camera

* Dependencies:
	* Kafka-python
	* OpenCV 2.4.10 for Raspberry Pi, OpenCV 3.3.0 for laptop
	* PiCamera
	
* Deployment instructions:
	* Start the Kafka server and change the IP of your Kafka server in all the codes before running!
 	* For sending from Raspberry pi camera:
		* Run rpi_kafka.py on your Raspberry Pi after setting up the camera.
	* For sending from Laptop camera:
		* Run send_video.py in your remote system.
	* For Receiving from any source:
		* Run receive_video_kafka.py in host system, the streamed video feed from remote host will appear in your system in a new window.

### Contribution guidelines ###

* https://www.pyimagesearch.com/2015/03/30/accessing-the-raspberry-pi-camera-with-opencv-and-python/
* https://pypi.python.org/pypi/kafka-python
* https://scotch.io/tutorials/build-a-distributed-streaming-system-with-apache-kafka-and-python
* Author : Sreekiran A R, sreekiranar@gmail.com
* Code Review : Jatan Sharma, jatansharma@gmail.com
