/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import static org.junit.Assert.assertEquals;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.junit.Test;

public class DateAndTimeTest {

    @Test
    public void test() {
        Date date = Util.parseDate("2016-08-07T12:23:48Z");
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(date.getTime());

        assertEquals(calendar.get(Calendar.MILLISECOND), 0);
        assertEquals(calendar.get(Calendar.MONTH), 7);
        assertEquals(calendar.get(Calendar.MINUTE), 23);

        date = Util.parseDate("2016-08-07T12:23:48.812Z");
        calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(date.getTime());
        assertEquals(calendar.get(Calendar.MILLISECOND), 812);
        assertEquals(calendar.get(Calendar.MONTH), 7);
        assertEquals(calendar.get(Calendar.MINUTE), 23);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalid_LocalTime() {
        Util.parseDate("2016-08-07T12:23:48.325-01:00");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalid_Grabage() {
        Util.parseDate("this-is-not-date-and-time");
    }

}
