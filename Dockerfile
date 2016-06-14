FROM williamyeh/java7
MAINTAINER https://github.com/dariusgm
# Need to update for ssh
RUN apt-get update && \
  apt-get install -y bash wget tar ssh

# Fetch Hadoop and Unpack & Fetch Hive and Unpack
WORKDIR /usr/local
RUN wget http://mirror.softaculous.com/apache/hadoop/common/hadoop-2.6.4/hadoop-2.6.4.tar.gz
RUN wget http://mirror.softaculous.com/apache/hive/hive-2.0.0/apache-hive-2.0.0-bin.tar.gz
RUN wget http://mirror.softaculous.com/apache/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz

RUN tar xf hadoop-2.6.4.tar.gz
RUN tar xf apache-hive-2.0.0-bin.tar.gz
RUN tar xf spark-1.6.1-bin-hadoop2.6.tgz

RUN ln -s /usr/local/hadoop-2.6.4 /usr/local/hadoop
RUN ln -s /usr/local/apache-hive-2.0.0-bin /usr/local/hive
RUN ln -s /usr/local/spark-1.6.1-bin-hadoop2.6 /usr/local/spark

WORKDIR /usr/local/hadoop/etc/hadoop

# Remove Templates
RUN rm -f core-site.xml hadoop-env.sh hdfs-site.xml mapred-site.xml yarn-site.xml

# Install core-site Config
ADD docker/core-site.xml core-site.xml
ADD docker/hadoop-env.sh hadoop-env.sh
ADD docker/hdfs-site.xml hdfs-site.xml
ADD docker/mapred-site.xml mapred-site.xml
ADD docker/yarn-site.xml yarn-site.xml
ADD docker/folk.tar.gz /tmp/folk

# Set Env

ENV HADOOP_HEAPSIZE=8192
ENV HADOOP_HOME=/usr/local/hadoop
ENV HIVE_HOME=/usr/local/hive
ENV PATH=$HIVE_HOME:$HADOOP_HOME:$PATH
ENV JAVA_HOME=/usr/

ENV SPARK_MASTER_IP=0.0.0.0
ENV SPARK_MASTER_WEBUI_PORT=7088
ENV SPARK_WORKER_PORT=7090

# Setup SSH
WORKDIR /root
RUN mkdir .ssh
RUN cat /dev/zero | ssh-keygen -q -N "" > /dev/null && cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys
RUN sed -i -e 's#^#export JAVA_HOME=/usr\n#' /etc/bash.bashrc

ADD docker/hive-site.xml /usr/local/hive/conf/hive-site.xml

# Format HFS
RUN /usr/local/hadoop/bin/hdfs namenode -format -nonInteractive

# Create Hive Metastore
RUN /usr/local/hive/bin/schematool -initSchema -dbType derby

# Hadoop Resource Manager
EXPOSE 8088

# Hadoop NameNode
EXPOSE 50070

# Hadoop DataNode
EXPOSE 50075

# Hive WebUI
EXPOSE 10002

# Hive Master
EXPOSE 10000

# Spark Master, MasterUI, WOrker, WorkerUI
EXPOSE 7077 7088 7090 8081

# Start sshd, allow ssh connection for pseudo distributed mode, yarn, datanode and namenode, hive2 - move this to docker compose.
ENTRYPOINT service ssh start && \
    ssh-keyscan localhost > /root/.ssh/known_hosts && \
    ssh-keyscan ::1 >> /root/.ssh/known_hosts && \
    ssh-keyscan 0.0.0.0 >> /root/.ssh/known_hosts && \
    /usr/local/hadoop/sbin/start-yarn.sh && \
    /usr/local/hadoop/sbin/start-dfs.sh && \
    /usr/local/hadoop/bin/hadoop fs -copyFromLocal /tmp/folk/folk.csv /folk.csv && \
    /usr/local/spark/sbin/start-master.sh && \
    /usr/local/spark/sbin/start-slave.sh spark://127.0.0.1:7077 && \
    /usr/local/hive/bin/hive --service hiveserver2
