package org.jboss.pnc.logprocessor.eventduration;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;

public class DateParser {
    public static final DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = DateTimeFormatter
            .ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSXXX")
            .withZone(ZoneId.systemDefault());

    public static Instant parseTime(String time) {
        DateTimeFormatter[] formatters = { DEFAULT_DATE_TIME_FORMATTER, DateTimeFormatter.ISO_INSTANT,
                DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSZ").withZone(ZoneId.systemDefault()) };
        for (DateTimeFormatter formatter : formatters) {
            try {
                TemporalAccessor accessor = formatter.parse(time);
                return Instant.from(accessor);
            } catch (DateTimeParseException e) {
                // try next one
            }
        }
        throw new DateTimeException("Invalid input datetime format [" + time + "]");
    }
}
