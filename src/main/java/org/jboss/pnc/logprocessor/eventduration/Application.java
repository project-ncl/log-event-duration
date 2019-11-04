package org.jboss.pnc.logprocessor.eventduration;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

/**
 * @author Ales Justin
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class Application {

    private KafkaStreams streams;

    public Application(
            Properties kafkaProperties,
            String inputTopic,
            String outputTopic,
            String durationsTopic) {
        LogProcessorTopology logProcessorTopology = new LogProcessorTopology(
                inputTopic,
                outputTopic,
                durationsTopic);

        Topology topology = logProcessorTopology.buildTopology(kafkaProperties);
        streams = new KafkaStreams(topology, kafkaProperties);
    }

    public void start() {
        streams.start();
    }

    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }
}
