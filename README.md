=== Installation ===
Build Image
===========

docker build -t spark .

Run container
=============
docker run -it -p 8088:8088 -p 50070:50070 -p 50075:50075 -p 10000:10000 -p 10002:10002 -p 7077:7077 -p 7088:7088 -p 7090:7090 -p 8081:8081 spark

Connect to Container
=============
docker exec spark bash


Deploy Spark App
================
IP=(docker-machine ip)
./bin/spark-submit --class FileProcessing --master spark://$IP:7077 --deploy-mode /target/scala-meetup-spark-1.0-SNAPSHOT.jar
