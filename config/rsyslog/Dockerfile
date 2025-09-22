FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y rsyslog rsyslog-kafka && \
    rm -rf /var/lib/apt/lists/*
COPY 01-kafka.conf /etc/rsyslog.d/
 
CMD ["rsyslogd", "-n", "-f", "/etc/rsyslog.conf"]
