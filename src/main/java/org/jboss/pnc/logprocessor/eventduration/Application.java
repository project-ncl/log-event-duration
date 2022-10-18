package org.jboss.pnc.logprocessor.eventduration;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import javax.enterprise.inject.Vetoed;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.instrumentation.kafkaclients.KafkaTelemetry;

/**
 * @author Ales Justin
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
@Vetoed
public class Application {

    private KafkaStreams streams;

    public Application(
            Properties kafkaProperties,
            String inputTopic,
            String outputTopic,
            Optional<String> durationsTopic) {
        LogProcessorTopology logProcessorTopology = new LogProcessorTopology(inputTopic, outputTopic, durationsTopic);

        Topology topology = logProcessorTopology.buildTopology(kafkaProperties);
        streams = new KafkaStreams(topology, kafkaProperties, new TracingKafkaClientSupplier());
    }

    public KafkaStreams.State getStreamState() {
        return streams.state();
    }

    public void start() {
        streams.start();
    }

    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }

    private static class TracingKafkaClientSupplier extends DefaultKafkaClientSupplier {
        @Override
        public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
            KafkaTelemetry telemetry = KafkaTelemetry.create(GlobalOpenTelemetry.get());
            return telemetry.wrap(super.getProducer(config));
        }

        @Override
        public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
            KafkaTelemetry telemetry = KafkaTelemetry.create(GlobalOpenTelemetry.get());
            return telemetry.wrap(super.getConsumer(config));
        }

        @Override
        public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
            return this.getConsumer(config);
        }

        @Override
        public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
            return this.getConsumer(config);
        }
    }
}
