package org.jboss.pnc.logprocessor.eventduration;

import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
@ApplicationScoped
public class InContainerBoot {

    @ConfigProperty(name = "kafkaPropertiesPath")
    String kafkaPropertiesPath;

    @ConfigProperty(name = "inputTopicName")
    String inputTopicName;

    @ConfigProperty(name = "outputTopicName")
    String outputTopicName;

    @ConfigProperty(name = "durationsTopicName")
    String durationsTopicName;

    Logger logger = Logger.getLogger(InContainerBoot.class);

    private Application application;

    public void init() throws IOException {
        Properties kafkaProperties = new Properties();
        try (FileInputStream kafkaPropertiesStream = new FileInputStream(kafkaPropertiesPath)) {
            kafkaProperties.load(kafkaPropertiesStream);
        } catch (IOException e) {
            logger.error("Cannot read Kafka.properties", e);
            throw e;
        }

        application = new Application(kafkaProperties, inputTopicName, outputTopicName, durationsTopicName);
        application.start();
    }

    public KafkaStreams.State getStreamState() {
        return application.getStreamState();
    }

    public void destroy() {
        application.stop();
    }
}
