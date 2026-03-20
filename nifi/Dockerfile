FROM apache/nifi:2.0.0

USER root

# Download HDFS NAR files for NiFi 2.0.0
RUN cd /opt/nifi/nifi-current/lib && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/nifi/nifi-hadoop-nar/2.0.0/nifi-hadoop-nar-2.0.0.nar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/nifi/nifi-hadoop-libraries-nar/2.0.0/nifi-hadoop-libraries-nar-2.0.0.nar && \
    chown nifi:nifi /opt/nifi/nifi-current/lib/*.nar

USER nifi

WORKDIR /opt/nifi/nifi-current
