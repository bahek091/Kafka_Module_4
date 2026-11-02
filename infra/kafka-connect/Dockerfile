FROM confluentinc/cp-kafka-connect:7.7.1

RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4

USER root
COPY jmx_prometheus_javaagent-0.17.2.jar /opt/
COPY kafka-connect.yml /opt/
USER appuser