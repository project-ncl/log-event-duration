package org.jboss.pnc.logprocessor.eventduration;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.jboss.pnc.logprocessor.eventduration.domain.LogEvent;
import org.jboss.pnc.logprocessor.eventduration.utils.LogEventFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;
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
    private static TestInputTopic<String, LogEvent> inputTopic;

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
        inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new LogEvent.JsonSerializer());
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

        // BEGIN event
        LogEvent startEvent = logEventFactory.getLogEvent(now, LogEvent.EventType.BEGIN, processContext, eventName);
        inputTopic.pipeInput(startEvent);

        // Noise to test matching
        Instant latter = now.plus(10, MILLIS);
        LogEvent startEventOther = logEventFactory
                .getLogEvent(latter, LogEvent.EventType.BEGIN, "other-context", eventName);
        inputTopic.pipeInput(startEventOther);

        // Noise to test matching
        LogEvent endEventOther = logEventFactory
                .getLogEvent(now.plus(20, MILLIS), LogEvent.EventType.END, "other-context", eventName);
        inputTopic.pipeInput(endEventOther);

        // END event
        LogEvent endEvent = logEventFactory.getLogEvent(later, LogEvent.EventType.END, processContext, eventName);
        inputTopic.pipeInput(endEvent);

        KeyValue<String, LogEvent> outputRecordStart = readOutput();
        Assertions.assertNotNull(outputRecordStart.value);
        Assertions.assertEquals(LogEvent.EventType.BEGIN, outputRecordStart.value.getEventType().get());

        // two non matching noise events
        readOutput();
        readOutput();

        KeyValue<String, LogEvent> outputRecordEnd = readOutputTopic("output-topic");
        Assertions.assertNotNull(outputRecordEnd.value);
        Assertions.assertEquals(LogEvent.EventType.END, outputRecordEnd.value.getEventType().get());
        int operationTook = (Integer) outputRecordEnd.value.getMessage().get(DURATION_KEY);
        Assertions.assertEquals(duration.toMillis(), operationTook);
    }

    private KeyValue<String, LogEvent> readOutput() {
        return readOutputTopic("output-topic");
    }

    @Test
    public void shouldGetStarAndEndWhenStartCameLate() throws JsonProcessingException {
        String processContext = UUID.randomUUID().toString();

        Instant now = Instant.now();
        Duration duration = Duration.of(15, SECONDS);
        Instant later = now.plus(duration);

        LogEvent endEvent = logEventFactory.getLogEvent(later, LogEvent.EventType.END, processContext, "");
        inputTopic.pipeInput(endEvent);

        LogEvent startEvent = logEventFactory.getLogEvent(now, LogEvent.EventType.BEGIN, processContext, "");
        inputTopic.pipeInput(startEvent);

        KeyValue<String, LogEvent> outputRecordStart = readOutputTopic("output-topic");
        Assertions.assertNotNull(outputRecordStart.value);
        Assertions.assertEquals(LogEvent.EventType.BEGIN, outputRecordStart.value.getEventType().get());

        KeyValue<String, LogEvent> outputRecordEnd = readOutputTopic("output-topic");
        Assertions.assertNotNull(outputRecordEnd.value);
        Assertions.assertEquals(LogEvent.EventType.END, outputRecordEnd.value.getEventType().get());
        int operationTook = (Integer) outputRecordEnd.value.getMessage().get(DURATION_KEY);
        Assertions.assertEquals(duration.toMillis(), operationTook);
    }

    @Test
    public void shouldSendOnlyEndEventWithDurationToDurationsTopic() throws JsonProcessingException {
        String processContext = UUID.randomUUID().toString();

        Instant now = Instant.now();
        Duration duration = Duration.of(15, SECONDS);
        Instant later = now.plus(duration);

        LogEvent startEvent = logEventFactory.getLogEvent(now, LogEvent.EventType.BEGIN, processContext, "");
        inputTopic.pipeInput(startEvent);

        LogEvent endEvent = logEventFactory.getLogEvent(later, LogEvent.EventType.END, processContext, "");
        inputTopic.pipeInput(endEvent);

        KeyValue<String, LogEvent> outputRecordEnd = readOutputTopic("durations-topic");
        Assertions.assertNotNull(outputRecordEnd.value);
        Assertions.assertEquals(LogEvent.EventType.END, outputRecordEnd.value.getEventType().get());
        int operationTook = (Integer) outputRecordEnd.value.getMessage().get(DURATION_KEY);
        Assertions.assertEquals(duration.toMillis(), operationTook);

        KeyValue<String, LogEvent> outputRecordStart = readOutputTopic("durations-topic");
        Assertions.assertNull(outputRecordStart);
    }

    @Test
    public void shouldNotMixEventsWithContextVariant() throws JsonProcessingException {
        String processContext = "build-123";
        String processContextVariant = "1";
        String eventName = "FINALIZING_EXECUTION";

        Instant now = Instant.now();
        Duration duration = Duration.of(15, SECONDS);
        Duration variantDuration = Duration.of(10, SECONDS);

        // BEGIN event
        LogEvent startEvent = logEventFactory.getLogEvent(now, LogEvent.EventType.BEGIN, processContext, eventName);
        inputTopic.pipeInput(startEvent);

        // BEGIN event with variant
        LogEvent startEventVariant = logEventFactory
                .getLogEvent(now, LogEvent.EventType.BEGIN, processContext, processContextVariant, eventName);
        inputTopic.pipeInput(startEventVariant);

        // Noise to test matching
        LogEvent endEventVariant = logEventFactory.getLogEvent(
                now.plus(variantDuration),
                LogEvent.EventType.END,
                processContext,
                processContextVariant,
                eventName);
        inputTopic.pipeInput(endEventVariant);

        // END event
        LogEvent endEvent = logEventFactory
                .getLogEvent(now.plus(duration), LogEvent.EventType.END, processContext, eventName);
        inputTopic.pipeInput(endEvent);

        // start event
        KeyValue<String, LogEvent> outputRecordStart = readOutput();
        Assertions.assertNotNull(outputRecordStart.value);
        Assertions.assertEquals(LogEvent.EventType.BEGIN, outputRecordStart.value.getEventType().get());

        // start variant event
        readOutput();

        // end variant event
        KeyValue<String, LogEvent> outputVariantRecordEnd = readOutput();
        Assertions.assertNotNull(outputVariantRecordEnd.value);
        Assertions.assertEquals(LogEvent.EventType.END, outputVariantRecordEnd.value.getEventType().get());
        int variantOperationTook = (Integer) outputVariantRecordEnd.value.getMessage().get(DURATION_KEY);
        Assertions.assertEquals(variantDuration.toMillis(), variantOperationTook);

        KeyValue<String, LogEvent> outputRecordEnd = readOutput();
        Assertions.assertNotNull(outputRecordEnd.value);
        Assertions.assertEquals(LogEvent.EventType.END, outputRecordEnd.value.getEventType().get());
        int operationTook = (Integer) outputRecordEnd.value.getMessage().get(DURATION_KEY);
        Assertions.assertEquals(duration.toMillis(), operationTook);
    }

    @Test
    public void shouldSetKeytoProcessMdcContextIfAbsent() throws Exception {
        String processContext = "build-123";
        String eventName = "Test 123";

        Instant now = Instant.now();

        // BEGIN event
        LogEvent startEvent = logEventFactory.getLogEvent(now, LogEvent.EventType.BEGIN, processContext, eventName);
        inputTopic.pipeInput(startEvent);

        // READ
        KeyValue<String, LogEvent> outputRecordEnd = readOutput();
        Assertions.assertNotNull(outputRecordEnd.value);
        Assertions.assertEquals(processContext, outputRecordEnd.key);
    }

    @Test
    public void shouldSetKeyToDefaultMessageKeyIfKeyNullAndProcessMdcContextNull() throws Exception {
        String eventName = "Test 123";

        Instant now = Instant.now();

        // BEGIN event
        LogEvent startEvent = logEventFactory.getLogEvent(now, LogEvent.EventType.BEGIN, null, eventName);
        inputTopic.pipeInput(startEvent);

        // READ
        KeyValue<String, LogEvent> outputRecordEnd = readOutput();
        Assertions.assertNotNull(outputRecordEnd.value);
        Assertions.assertEquals(MergeTransformer.DEFAULT_KAFKA_MESSAGE_KEY, outputRecordEnd.key);

    }

    private KeyValue<String, LogEvent> readOutputTopic(String topic) {
        TestOutputTopic<String, LogEvent> outputTopic = testDriver
                .createOutputTopic(topic, new StringDeserializer(), new LogEvent.JsonDeserializer());
        try {
            return outputTopic.readKeyValue();
        } catch (NoSuchElementException e) {
            return null;
        }
    }
}
