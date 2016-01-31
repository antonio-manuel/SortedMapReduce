#!/bin/bash

DOCKER_CONTAINER_HOME=<Container_Path>

cd $DOCKER_CONTAINER_HOME

##script taken from start-container.sh
# run N slave containers
N=$1

# the defaut node number is 3
if [ $# = 0 ]
then
	N=3
fi
	
# delete old master container and start new master container
docker rm -f master &> /dev/null
echo "start master container..."
docker run -d -t --dns 127.0.0.1 -P --name master -h master.kiwenlau.com -w /root kiwenlau/hadoop-master:0.1.0 &> /dev/null

# get the IP address of master container
FIRST_IP=$(docker inspect --format="{{.NetworkSettings.IPAddress}}" master)

# delete old slave containers and start new slave containers
i=1
while [ $i -lt $N ]
do
	docker rm -f slave$i &> /dev/null
	echo "start slave$i container..."
	docker run -d -t --dns 127.0.0.1 -P --name slave$i -h slave$i.kiwenlau.com -e JOIN_IP=$FIRST_IP kiwenlau/hadoop-slave:0.1.0 &> /dev/null
	((i++))
done 


#start hadoop
docker exec -it master ./start-hadoop.sh

#copy jar to container
docker cp SortedWordCounter.jar master:/root/SortedWordCounter.jar

echo "Execute this command: hadoop jar ./SortedWordCounter.jar eu.antoniolopez.mapreduce.SortedWordCount http://www.gutenberg.org/ebooks/2600.txt.utf-8 ftp://<user>:<password>@<host>/<path+filename>;type=i

docker exec -it master bash
