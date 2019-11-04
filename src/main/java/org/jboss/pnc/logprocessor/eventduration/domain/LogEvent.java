package org.jboss.pnc.logprocessor.eventduration.domain;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class LogEvent {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String PROCESS_CONTEXT_KEY = "mdc.processContext";

    public static final String EVENT_NAME_KEY = "mdc.process_stage_name";

    public static final String EVENT_TYPE_KEY = "mdc.process_stage_step";

    public static final String TIMESTAMP_KEY = "@timestamp";

    public static final String MESSAGE_KEY = "message";

    public static final String DURATION_KEY = "operationTook";

    private Instant time;

    public String getIdentifier() {
        return message.get(PROCESS_CONTEXT_KEY) + "--" + message.get(EVENT_NAME_KEY);
    }

    public Instant getTime() {
        return time;
    }

    public Map<String, Object> getMessage() {
        return message;
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
            throw new SerializationException("Cannot construct object from serialized bytes.", e);
        }
        init(jsonNode);
    }

    public LogEvent(String serializedLogEvent) {
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(serializedLogEvent);
        } catch (IOException e) {
            throw new SerializationException("Cannot construct object from serialized string.", e);
        }
        init(jsonNode);
    }

    private void init(JsonNode jsonNode) {
        message = objectMapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {});
        String time = (String) message.get(TIMESTAMP_KEY);
        this.time = Instant.ofEpochMilli(Long.parseLong(time));
    }

    public void addDuration(Duration duration) {
        String logMessage = (String) message.get(MESSAGE_KEY);
        long durationMillis = duration.toMillis();
        message.put(MESSAGE_KEY, logMessage + " Took " + durationMillis + "ms.");
        message.put(DURATION_KEY, durationMillis);
    }

    public Optional<EventType> getEventType() {
        String eventType = (String) message.get(EVENT_TYPE_KEY);
        if (eventType != null) {
            return Optional.of(EventType.valueOf(eventType));
        } else {
            return Optional.empty();
        }
    }

    public static class JsonSerializer implements Serializer<LogEvent> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, LogEvent logEvent) {
            if (logEvent == null) {
                return null;
            }

            try {
                return objectMapper.writeValueAsBytes(logEvent.message);
            } catch (Exception e) {
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

        @Override
        public LogEvent deserialize(String topic, byte[] bytes) {
            if (bytes == null) {
                return null;
            }

            LogEvent data;
            try {
                data = new LogEvent(bytes);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
            return data;
        }

        @Override
        public void close() {
        }
    }
}
