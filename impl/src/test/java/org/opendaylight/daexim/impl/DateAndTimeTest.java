/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

class DateAndTimeTest {
    @Test
    void test() {
        var date = Util.parseDate("2016-08-07T12:23:48Z");
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(date.getTime());

        assertEquals(0, calendar.get(Calendar.MILLISECOND));
        assertEquals(7, calendar.get(Calendar.MONTH));
        assertEquals(23, calendar.get(Calendar.MINUTE));

        date = Util.parseDate("2016-08-07T12:23:48.812Z");
        calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(date.getTime());
        assertEquals(812, calendar.get(Calendar.MILLISECOND));
        assertEquals(7, calendar.get(Calendar.MONTH));
        assertEquals(23, calendar.get(Calendar.MINUTE));
    }

    @Test
    void testInvalid_LocalTime() {
        final var ex = assertThrows(IllegalArgumentException.class,
            () -> Util.parseDate("2016-08-07T12:23:48.325-01:00"));
        assertEquals("Unrecognized DateAndTime value : 2016-08-07T12:23:48.325-01:00 (only UTC date is accepted)",
            ex.getMessage());
    }

    @Test
    void testInvalid_Grabage() {
        final var ex = assertThrows(IllegalArgumentException.class,
            () -> Util.parseDate("this-is-not-date-and-time"));
        assertEquals("Unrecognized DateAndTime value : this-is-not-date-and-time (only UTC date is accepted)",
            ex.getMessage());
    }
}
