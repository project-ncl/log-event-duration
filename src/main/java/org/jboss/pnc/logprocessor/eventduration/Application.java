package org.jboss.pnc.logprocessor.eventduration;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import javax.enterprise.inject.Vetoed;
import java.util.Optional;
import java.util.Properties;

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
        streams = new KafkaStreams(topology, kafkaProperties);

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
}
