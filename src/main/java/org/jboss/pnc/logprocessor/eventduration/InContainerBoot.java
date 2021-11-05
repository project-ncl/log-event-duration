package org.jboss.pnc.logprocessor.eventduration;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;
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
    Optional<String> durationsTopicName;

    private static final String className = InContainerBoot.class.getName();

    Logger logger = Logger.getLogger(InContainerBoot.class);

    private Application application;

    @Inject
    MeterRegistry registry;

    private Counter errCounter;

    @PostConstruct
    void initMetrics() {
        errCounter = registry.counter(className + ".error.count");
    }

    @Timed
    public void init() throws IOException {
        Properties kafkaProperties = new Properties();
        try (FileInputStream kafkaPropertiesStream = new FileInputStream(kafkaPropertiesPath)) {
            kafkaProperties.load(kafkaPropertiesStream);
        } catch (IOException e) {
            errCounter.increment();
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
