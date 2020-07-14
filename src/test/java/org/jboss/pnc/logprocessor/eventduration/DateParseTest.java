package org.jboss.pnc.logprocessor.eventduration;

import org.junit.jupiter.api.Test;

public class DateParseTest {

    @Test
    public void shouldParseDates() {
        String time = "2020-07-06T16:23:55.149+0200";
        System.out.println(DateParser.parseTime(time));

        time = "2020-07-06T16:23:55.149+02:00";
        System.out.println(DateParser.parseTime(time));

        time = "2020-07-06T16:23:55.149Z";
        System.out.println(DateParser.parseTime(time));
    }
}
