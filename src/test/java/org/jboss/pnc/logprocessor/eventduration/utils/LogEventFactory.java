package org.jboss.pnc.logprocessor.eventduration.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.pnc.logprocessor.eventduration.domain.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.jboss.pnc.logprocessor.eventduration.domain.LogEvent.EVENT_TYPE_KEY;
import static org.jboss.pnc.logprocessor.eventduration.domain.LogEvent.PROCESS_CONTEXT_KEY;
import static org.jboss.pnc.logprocessor.eventduration.domain.LogEvent.TIMESTAMP_KEY;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class LogEventFactory {

    private static final Logger logger = LoggerFactory.getLogger(LogEventFactory.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public LogEvent getLogEvent(Instant now, LogEvent.EventType eventType, String processContext) throws JsonProcessingException {
        Map<String, String> map = new HashMap<>();
        map.put("customKey", "passThrough");
        map.put(TIMESTAMP_KEY, Long.toString(now.toEpochMilli()));
        map.put(EVENT_TYPE_KEY, eventType.toString());
        map.put(PROCESS_CONTEXT_KEY, processContext);

        String serializedInput = objectMapper.writeValueAsString(map);
        logger.info("Serialized input {}", serializedInput);

        return new LogEvent(serializedInput);
    }

}
