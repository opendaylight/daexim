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

import java.util.List;

import org.junit.Test;
import org.opendaylight.daexim.impl.Util;
import org.opendaylight.daexim.impl.model.internal.Model;

public class MetadataParsingTest {
    @Test
    public void testParseModules() {
        List<Model> models = Util.parseModels(this.getClass().getResourceAsStream('/'+ Util.FILE_PREFIX + "models.json"));
        assertEquals(28, models.size());
        assertEquals("2015-08-04", models.get(0).getRevision());
        assertEquals("urn:opendaylight:params:xml:ns:yang:controller:md:sal:clustering:entity-owners",
                models.get(0).getNamespace());
        assertEquals("entity-owners", models.get(0).getModule());
    }
}
