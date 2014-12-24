#!/bin/bash

sudo -u hdfs hadoop fs -rm -R /user/hdfs/jar
sudo -u hdfs hadoop fs -rm -R /user/hdfs/Input
sudo -u hdfs hadoop fs -mkdir /user/hdfs/jar

sudo -u hdfs hadoop fs -mkdir /user/hdfs/Input

sudo -u hdfs hadoop fs -cp /user/reviewuser/Jar/ItemBasedReco-0.0.1-SNAPSHOT.jar /user/hdfs/jar/

sudo -u hdfs hadoop fs -cp /user/reviewuser/Input/u1.base /user/hdfs/Input/


cd ~
rm -R ~/Jar
mkdir ~/Jar
chmod -R 777 ~/Jar
sudo -u hdfs hadoop fs -copyToLocal /user/reviewuser/Jar/ItemBasedReco-0.0.1-SNAPSHOT.jar ~/Jar/
sudo -u hdfs hadoop jar ~/Jar/ItemBasedReco-0.0.1-SNAPSHOT.jar com.naveen.drivers.DataScrub 1 2

sudo -u hdfs hadoop jar ~/Jar/ItemBasedReco-0.0.1-SNAPSHOT.jar com.naveen.drivers.CoOccurrenceMatrix 1 2
sudo -u hdfs hadoop jar ~/Jar/ItemBasedReco-0.0.1-SNAPSHOT.jar com.naveen.drivers.ItemVectorDriver 1 2

sudo -u hdfs hadoop jar ~/Jar/ItemBasedReco-0.0.1-SNAPSHOT.jar com.naveen.drivers.MergeMatrixItemDriver 1 2


sudo -u hdfs hadoop jar ~/Jar/ItemBasedReco-0.0.1-SNAPSHOT.jar com.naveen.drivers.MatrixProductDriver 1 2