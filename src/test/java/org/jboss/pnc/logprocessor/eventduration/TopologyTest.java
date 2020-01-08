package org.jboss.pnc.logprocessor.eventduration;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.jboss.pnc.logprocessor.eventduration.domain.LogEvent;
import org.jboss.pnc.logprocessor.eventduration.utils.LogEventFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.jboss.pnc.logprocessor.eventduration.domain.LogEvent.DURATION_KEY;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class TopologyTest {

    private LogEventFactory logEventFactory = new LogEventFactory();

    private static TopologyTestDriver testDriver;
    private static ConsumerRecordFactory<String, LogEvent> recordFactory;

    @BeforeEach
    public void setup() {
        // setup test driver
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        LogProcessorTopology logProcessorTopology = new LogProcessorTopology(
                "input-topic",
                "output-topic",
                java.util.Optional.of("durations-topic"));

        Topology topology = logProcessorTopology.buildTopology(props);

        testDriver = new TopologyTestDriver(topology, props);

        recordFactory = new ConsumerRecordFactory<>(
                "input-topic",
                new StringSerializer(),
                new LogEvent.JsonSerializer());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void shouldAddDurationToEndEvents() throws JsonProcessingException {
        String processContext = "build-123";
        String eventName = "FINALIZING_EXECUTION";

        Instant now = Instant.now();
        Duration duration = Duration.of(15, SECONDS);
        Instant later = now.plus(duration);

        //BEGIN event
        LogEvent startEvent = logEventFactory.getLogEvent(now, LogEvent.EventType.BEGIN, processContext, eventName);
        testDriver.pipeInput(recordFactory.create("input-topic", null, startEvent));

        //Noise to test matching
        Instant latter = now.plus(10, MILLIS);
        LogEvent startEventOther = logEventFactory.getLogEvent(latter, LogEvent.EventType.BEGIN, "other-context", eventName);
        testDriver.pipeInput(recordFactory.create("input-topic", null, startEventOther));

        //Noise to test matching
        LogEvent endEventOther = logEventFactory.getLogEvent(now.plus(20, MILLIS), LogEvent.EventType.END, "other-context", eventName);
        testDriver.pipeInput(recordFactory.create("input-topic", null, endEventOther));

        //END event
        LogEvent endEvent = logEventFactory.getLogEvent(later, LogEvent.EventType.END, processContext, eventName);
        testDriver.pipeInput(recordFactory.create("input-topic", null, endEvent));

        ProducerRecord<String, LogEvent> outputRecordStart = readOutput();
        Assertions.assertNotNull(outputRecordStart.value());
        Assertions.assertEquals(LogEvent.EventType.BEGIN, outputRecordStart.value().getEventType().get());

        //two non matching noise events
        readOutput();
        readOutput();

        ProducerRecord<String, LogEvent> outputRecordEnd = readOutputTopic("output-topic");
        Assertions.assertNotNull(outputRecordEnd.value());
        Assertions.assertEquals(LogEvent.EventType.END, outputRecordEnd.value().getEventType().get());
        int operationTook = (Integer) outputRecordEnd.value().getMessage().get(DURATION_KEY);
        Assertions.assertEquals(duration.toMillis(), operationTook);
    }

    private ProducerRecord<String, LogEvent> readOutput() {
        return readOutputTopic("output-topic");
    }

    @Test
    public void shouldGetStarAndEndWhenStartCameLate() throws JsonProcessingException {
        String processContext = UUID.randomUUID().toString();

        Instant now = Instant.now();
        Duration duration = Duration.of(15, SECONDS);
        Instant later = now.plus(duration);

        LogEvent endEvent = logEventFactory.getLogEvent(later, LogEvent.EventType.END, processContext, "");
        testDriver.pipeInput(recordFactory.create("input-topic", null, endEvent));

        LogEvent startEvent = logEventFactory.getLogEvent(now, LogEvent.EventType.BEGIN, processContext, "");
        testDriver.pipeInput(recordFactory.create("input-topic", null, startEvent));

        ProducerRecord<String, LogEvent> outputRecordStart = readOutputTopic("output-topic");
        Assertions.assertNotNull(outputRecordStart.value());
        Assertions.assertEquals(LogEvent.EventType.BEGIN, outputRecordStart.value().getEventType().get());

        ProducerRecord<String, LogEvent> outputRecordEnd = readOutputTopic("output-topic");
        Assertions.assertNotNull(outputRecordEnd.value());
        Assertions.assertEquals(LogEvent.EventType.END, outputRecordEnd.value().getEventType().get());
        int operationTook = (Integer) outputRecordEnd.value().getMessage().get(DURATION_KEY);
        Assertions.assertEquals(duration.toMillis(), operationTook);
    }

    @Test
    public void shouldSendOnlyEndEventWithDurationToDurationsTopic() throws JsonProcessingException {
        String processContext = UUID.randomUUID().toString();

        Instant now = Instant.now();
        Duration duration = Duration.of(15, SECONDS);
        Instant later = now.plus(duration);

        LogEvent startEvent = logEventFactory.getLogEvent(now, LogEvent.EventType.BEGIN, processContext, "");
        testDriver.pipeInput(recordFactory.create("input-topic", null, startEvent));

        LogEvent endEvent = logEventFactory.getLogEvent(later, LogEvent.EventType.END, processContext, "");
        testDriver.pipeInput(recordFactory.create("input-topic", null, endEvent));

        ProducerRecord<String, LogEvent> outputRecordEnd = readOutputTopic("durations-topic");
        Assertions.assertNotNull(outputRecordEnd.value());
        Assertions.assertEquals(LogEvent.EventType.END, outputRecordEnd.value().getEventType().get());
        int operationTook = (Integer) outputRecordEnd.value().getMessage().get(DURATION_KEY);
        Assertions.assertEquals(duration.toMillis(), operationTook);

        ProducerRecord<String, LogEvent> outputRecordStart = readOutputTopic("durations-topic");
        Assertions.assertNull(outputRecordStart);
    }

    private ProducerRecord<String, LogEvent> readOutputTopic(String topic) {
        return testDriver.readOutput(topic, new StringDeserializer(), new LogEvent.JsonDeserializer());
    }
}
