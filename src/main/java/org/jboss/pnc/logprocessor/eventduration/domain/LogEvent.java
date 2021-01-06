package org.jboss.pnc.logprocessor.eventduration.domain;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.Counter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.eclipse.microprofile.metrics.MetricUnits;
import org.eclipse.microprofile.metrics.annotation.Gauge;
import org.eclipse.microprofile.metrics.annotation.Timed;
import org.jboss.pnc.logprocessor.eventduration.DateParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
@Timed
public class LogEvent {

    static final Counter exceptionsTotal = Counter.build()
            .name("LogEvent_Exceptions_Total")
            .help("Errors and Warnings counting metric")
            .labelNames("severity")
            .register();

    private static final Logger logger = LoggerFactory.getLogger(LogEvent.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String MDC_KEY = "mdc";

    public static final String MDC_PROCESS_CONTEXT_KEY = "processContext";

    public static final String MDC_EVENT_NAME_KEY = "process_stage_name";

    public static final String MDC_EVENT_TYPE_KEY = "process_stage_step";

    public static final String TIMESTAMP_KEY = "@timestamp";

    public static final String MESSAGE_KEY = "message";

    public static final String DURATION_KEY = "operationTook";

    public static final String KAFKA_KEY = "kafkaKey";

    private Instant time;

    public String getIdentifier() {
        Map<String, String> mdc = (Map<String, String>) message.get(MDC_KEY);
        if (mdc != null) {
            String processContext = mdc.get(MDC_PROCESS_CONTEXT_KEY);
            String eventName = mdc.get(MDC_EVENT_NAME_KEY);
            if (processContext == null || processContext.equals("")) {
                exceptionsTotal.labels("warning").inc();
                logger.warn("Missing processContext for event {}.", eventName);
            }
            return processContext + "--" + eventName;
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
            exceptionsTotal.labels("error").inc();
            throw new SerializationException("Cannot construct object from serialized bytes.", e);
        }
        init(jsonNode);
    }

    public LogEvent(String serializedLogEvent) {
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(serializedLogEvent);
        } catch (IOException e) {
            exceptionsTotal.labels("error").inc();
            throw new SerializationException("Cannot construct object from serialized string.", e);
        }
        init(jsonNode);
    }

    private void init(JsonNode jsonNode) {
        message = objectMapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        logger.trace("New log event {}.", message);
        String time = (String) message.get(TIMESTAMP_KEY);

        this.time = DateParser.parseTime(time);
    }

    public void addDuration(Duration duration) {
        String logMessage = (String) message.get(MESSAGE_KEY);
        long durationMillis = duration.toMillis();
        message.put(MESSAGE_KEY, logMessage + " Took " + durationMillis + "ms.");
        message.put(DURATION_KEY, durationMillis);
    }

    public Optional<EventType> getEventType() {
        Map<String, String> mdc = (Map<String, String>) message.get(MDC_KEY);
        if (mdc != null) {
            String eventType = mdc.get(MDC_EVENT_TYPE_KEY);
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

        @Override
        public byte[] serialize(String topic, LogEvent logEvent) {
            if (logEvent == null) {
                return null;
            }

            try {
                return objectMapper.writeValueAsBytes(logEvent.message);
            } catch (Exception e) {
                exceptionsTotal.labels("error").inc();
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
            if (bytes == null || bytes.length == 0) {
                return null;
            }

            LogEvent data;
            try {
                data = new LogEvent(bytes);
            } catch (Exception e) {
                exceptionsTotal.labels("error").inc();
                throw new SerializationException(e);
            }
            return data;
        }

        @Override
        public void close() {
        }
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Gauge(name = "LogEvent_Err_Count", unit = MetricUnits.NONE, description = "Errors count")
    public int showCurrentErrCount() {
        return (int) exceptionsTotal.labels("error").get();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Gauge(name = "LogEvent_Warn_Count", unit = MetricUnits.NONE, description = "Warnings count")
    public int showCurrentWarnCount() {
        return (int) exceptionsTotal.labels("warning").get();
    }
}
