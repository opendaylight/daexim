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

import org.junit.Test;
import org.opendaylight.daexim.impl.Util;

public class DateAndTimeTest {
    @Test
    public void test() {
        Date d;
        Calendar c;
        d = Util.parseDate("2016-08-07T12:23:48Z");
        c = Calendar.getInstance();
        c.setTimeInMillis(d.getTime());

        assertEquals(c.get(Calendar.MILLISECOND), 0);
        assertEquals(c.get(Calendar.MONTH), 7);
        assertEquals(c.get(Calendar.MINUTE), 23);

        d = Util.parseDate("2016-08-07T12:23:48.812Z");
        c = Calendar.getInstance();
        c.setTimeInMillis(d.getTime());
        assertEquals(c.get(Calendar.MILLISECOND), 812);
        assertEquals(c.get(Calendar.MONTH), 7);
        assertEquals(c.get(Calendar.MINUTE), 23);
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
