package org.jboss.pnc.logprocessor.eventduration.domain;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.jboss.pnc.logprocessor.eventduration.DateParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.jboss.pnc.api.constants.MDCKeys.EVENT_NAME_KEY;
import static org.jboss.pnc.api.constants.MDCKeys.EVENT_TYPE_KEY;
import static org.jboss.pnc.api.constants.MDCKeys.PROCESS_CONTEXT_KEY;
import static org.jboss.pnc.api.constants.MDCKeys.PROCESS_CONTEXT_VARIANT_KEY;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class LogEvent {

    private static final String className = LogEvent.class.getName();

    private static final Logger logger = LoggerFactory.getLogger(LogEvent.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String MDC_KEY = "mdc";

    public static final String TIMESTAMP_KEY = "timestamp";

    public static final String AT_TIMESTAMP_KEY = "@timestamp";

    public static final String MESSAGE_KEY = "message";

    public static final String DURATION_KEY = "operationTook";

    public static final String KAFKA_KEY = "kafkaKey";

    private Instant time;

    @Inject
    @Singleton
    MeterRegistry registry;

    private static Counter errCounter;
    private static Counter warnCounter;

    @PostConstruct
    void initMetrics() {
        errCounter = registry.counter(className + ".error.count");
        warnCounter = registry.counter(className + ".warning.count");
    }

    public String getIdentifier() {
        Map<String, String> mdc = (Map<String, String>) message.get(MDC_KEY);
        if (mdc != null) {
            String processContext = mdc.get(PROCESS_CONTEXT_KEY);
            String eventName = mdc.get(EVENT_NAME_KEY);
            if (processContext == null || processContext.equals("")) {
                warnCounter.increment();
                logger.warn("Missing processContext for event {}.", eventName);
            }
            String processContextVariant = mdc.get(PROCESS_CONTEXT_VARIANT_KEY);
            if (processContextVariant != null && !processContextVariant.equals("")) {
                return processContext + "--" + processContextVariant + "--" + eventName;
            } else {
                return processContext + "--" + eventName;
            }
        } else {
            return "";
        }
    }

    public Instant getTime() {
        return time;
    }

    public Map<String, Object> getMessage() {
        return message;
    }

    public String getKafkaKey() {
        return (String) message.get(KAFKA_KEY);
    }

    public void setKafkaKey(String key) {
        message.put(KAFKA_KEY, key);
    }

    public enum EventType {
        BEGIN, END;
    }

    private Map<String, Object> message;

    public LogEvent(byte[] serialized) {
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(serialized);
        } catch (IOException e) {
            errCounter.increment();
            throw new SerializationException("Cannot construct object from serialized bytes.", e);
        }
        init(jsonNode);
    }

    public LogEvent(String serializedLogEvent) {
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(serializedLogEvent);
        } catch (IOException e) {
            errCounter.increment();
            throw new SerializationException("Cannot construct object from serialized string.", e);
        }
        init(jsonNode);
    }

    private void init(JsonNode jsonNode) {
        message = objectMapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        logger.trace("New log event {}.", message);
        String time;
        if (message.containsKey(TIMESTAMP_KEY)) {
            time = (String) message.get(TIMESTAMP_KEY);
        } else {
            time = (String) message.get(AT_TIMESTAMP_KEY);
        }

        this.time = DateParser.parseTime(time);
    }

    public void addDuration(Duration duration) {
        String logMessage = (String) message.get(MESSAGE_KEY);
        long durationMillis = duration.toMillis();
        message.put(MESSAGE_KEY, logMessage + " Took " + durationMillis + "ms.");
        message.put(DURATION_KEY, durationMillis);
        /*
         * message is sent to Kafka. For test TC46, we'd also like to print it to stdout so that we can observe what is
         * going on in Splunk
         */
        Map<String, String> mdc = (Map<String, String>) message.get(MDC_KEY);
        if (mdc != null) {
            String processContext = mdc.get(PROCESS_CONTEXT_KEY);
            logger.info("Process [{}]: {} Took {} ms.", processContext, logMessage, durationMillis);
        } else {
            logger.info("{} Took {} ms.", logMessage, durationMillis);
        }
    }

    public Optional<EventType> getEventType() {
        Map<String, String> mdc = (Map<String, String>) message.get(MDC_KEY);
        if (mdc != null) {
            String eventType = mdc.get(EVENT_TYPE_KEY);
            if (eventType != null) {
                return Optional.of(EventType.valueOf(eventType));
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    public static class JsonSerializer implements Serializer<LogEvent> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Timed
        @Override
        public byte[] serialize(String topic, LogEvent logEvent) {
            if (logEvent == null) {
                return null;
            }

            try {
                return objectMapper.writeValueAsBytes(logEvent.message);
            } catch (Exception e) {
                errCounter.increment();
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {
        }
    }

    public static class JsonDeserializer implements Deserializer<LogEvent> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Timed
        @Override
        public LogEvent deserialize(String topic, byte[] bytes) {
            if (bytes == null || bytes.length == 0) {
                return null;
            }

            LogEvent data;
            try {
                data = new LogEvent(bytes);
            } catch (Exception e) {
                errCounter.increment();
                throw new SerializationException(e);
            }
            return data;
        }

        @Override
        public void close() {
        }
    }
}
