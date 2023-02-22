FROM confluentinc/cp-kafka:latest

WORKDIR /usr/bin

ADD --chown=appuser:appuser jmx_exporter.yml jmx_exporter.yml

ADD --chown=appuser:appuser jmx_prometheus_javaagent-0.17.2.jar jmx_prometheus_javaagent-0.17.2.jar

# Does not work... jvm or classpath troubles while mounting jar at image build stage... looks not accessible in classpath, meanwhile I used volume to workaround, it works BUT WHY ?