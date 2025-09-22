FROM rsyslog/rsyslog_base_ubuntu:latest

RUN apt-get update && apt-get install -y \
    rsyslog-kafka \
    && rm -rf /var/lib/apt/lists/*

COPY rsyslog.conf /etc/rsyslog.d/

CMD ["rsyslogd", "-n"]
