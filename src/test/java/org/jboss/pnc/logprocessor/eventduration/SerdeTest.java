package org.jboss.pnc.logprocessor.eventduration;

import java.time.Instant;

import org.jboss.pnc.logprocessor.eventduration.domain.LogEvent;
import org.jboss.pnc.logprocessor.eventduration.utils.LogEventFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class SerdeTest {

    private static final Logger logger = LoggerFactory.getLogger(SerdeTest.class);

    private LogEventFactory logEventFactory = new LogEventFactory();

    @Test
    public void shouldSerializeAndDeserializeLogEvent() throws JsonProcessingException {
        Instant now = Instant.ofEpochMilli(System.currentTimeMillis());

        LogEvent logEvent = logEventFactory.getLogEvent(now, LogEvent.EventType.BEGIN, "123-abc", "");
        Assertions.assertEquals(logEvent.getEventType().get(), LogEvent.EventType.BEGIN);
        logEvent.setKafkaKey("abc");

        byte[] serialized = new LogEvent.JsonSerializer().serialize("", logEvent);

        logger.info("Serialized {}", new String(serialized));

        new LogEvent.JsonDeserializer().deserialize("", serialized);

        Assertions.assertEquals(LogEvent.EventType.BEGIN, logEvent.getEventType().get());
        Assertions.assertEquals(now, logEvent.getTime());
        Assertions.assertEquals("abc", logEvent.getKafkaKey());
    }

}
