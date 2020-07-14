package org.jboss.pnc.logprocessor.eventduration.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.pnc.logprocessor.eventduration.DateParser;
import org.jboss.pnc.logprocessor.eventduration.domain.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.jboss.pnc.logprocessor.eventduration.domain.LogEvent.MDC_EVENT_NAME_KEY;
import static org.jboss.pnc.logprocessor.eventduration.domain.LogEvent.MDC_EVENT_TYPE_KEY;
import static org.jboss.pnc.logprocessor.eventduration.domain.LogEvent.MDC_PROCESS_CONTEXT_KEY;
import static org.jboss.pnc.logprocessor.eventduration.domain.LogEvent.TIMESTAMP_KEY;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class LogEventFactory {

    private static final Logger logger = LoggerFactory.getLogger(LogEventFactory.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public LogEvent getLogEvent(Instant instant, LogEvent.EventType eventType, String processContext, String eventName)
            throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        map.put("customKey", "passThrough");
        map.put(TIMESTAMP_KEY, DateParser.DEFAULT_DATE_TIME_FORMATTER.format(instant));

        Map<String, String> mdc = new HashMap<>();
        mdc.put(MDC_EVENT_TYPE_KEY, eventType.toString());
        mdc.put(MDC_EVENT_NAME_KEY, eventName);
        mdc.put(MDC_PROCESS_CONTEXT_KEY, processContext);
        map.put("mdc", mdc);

        String serializedInput = objectMapper.writeValueAsString(map);
        logger.info("Serialized input {}", serializedInput);

        return new LogEvent(serializedInput);
    }

}
