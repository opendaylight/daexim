/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.Map;
import java.util.function.Consumer;
import org.eclipse.jdt.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.mdsal.binding.dom.adapter.test.AbstractDataBrokerTest;
import org.opendaylight.mdsal.common.api.LogicalDatastoreType;
import org.opendaylight.mdsal.dom.api.DOMSchemaService;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev130715.YangIdentifier;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataStore;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModules;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModules.ModuleName;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModulesBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModulesKey;
import org.opendaylight.yangtools.yang.binding.CodeHelpers;
import org.opendaylight.yangtools.yang.common.QName;

public class ModuleExclusionTest extends AbstractDataBrokerTest {
    private static final String REV1 = "2016-09-13";

    private static final @Nullable Map<ExcludedModulesKey, ExcludedModules> EXCL_CFG = CodeHelpers
            .compatMap(ImmutableList.<ExcludedModules>builder()
                    .add(new ExcludedModulesBuilder().setDataStore(new DataStore("config"))
                            .setModuleName(new ModuleName(new YangIdentifier("A")))
                            .build())
                    .build());

    private static final @Nullable Map<ExcludedModulesKey, ExcludedModules> EXCL_OP = CodeHelpers
            .compatMap(ImmutableList.<ExcludedModules>builder()
                    .add(new ExcludedModulesBuilder().setDataStore(new DataStore("operational"))
                            .setModuleName(new ModuleName(new YangIdentifier("A")))
                            .build())
                    .build());

    private static final String REV2 = "2016-09-12";
    @SuppressWarnings("unchecked")
    private Consumer<Void> callback = mock(Consumer.class);
    private DOMSchemaService schemaService;

    @Before
    public void setUp() throws Exception {
        schemaService = mock(DOMSchemaService.class);
        when(schemaService.getGlobalContext()).thenReturn(getSchemaContext());
    }

    @After
    public void tearDown() {
        reset(schemaService);
    }


    @Test
    public void test() throws Exception {
        ExportTask task;
        // 'config' node at A@R1 is excluded by [{"data-store": "config", "module-name": "A"}]
        task = new ExportTask(null, EXCL_CFG, true, false, getDomBroker(), schemaService, callback);
        assertTrue(task.isExcluded(LogicalDatastoreType.CONFIGURATION, QName.create("testA", REV1, "A")));

        // 'operational' node at A@R1 is excluded by [{"data-store": "operational", "module-name": "A"}]
        task = new ExportTask(null, EXCL_OP, true, false, getDomBroker(), schemaService, callback);
        assertTrue(task.isExcluded(LogicalDatastoreType.OPERATIONAL, QName.create("testA", REV1, "A")));

        // 'config' node at A@R2 is excluded by [{"data-store": "config", "module-name": "A"}]
        task = new ExportTask(null, EXCL_CFG, true, false, getDomBroker(), schemaService, callback);
        assertTrue(task.isExcluded(LogicalDatastoreType.CONFIGURATION, QName.create("testA", REV2, "A")));

        // 'operational' node at A@R1 is excluded by [{"data-store": "operational", "module-name": "A"}]
        task = new ExportTask(null, EXCL_OP, true, false, getDomBroker(), schemaService, callback);
        assertTrue(task.isExcluded(LogicalDatastoreType.OPERATIONAL, QName.create("testA", REV1, "A")));

        // 'config' node at A@R1 is NOT excluded by [{"data-store": "operational", "module-name": "A"}]
        task = new ExportTask(null, EXCL_OP, true, false, getDomBroker(), schemaService, callback);
        assertFalse(task.isExcluded(LogicalDatastoreType.CONFIGURATION, QName.create("testA", REV1, "A")));

        //'operational' node at A@R1 is NOT excluded by [{"data-store": "config", "module-name": "A"}]
        task = new ExportTask(null, EXCL_CFG, true, false, getDomBroker(), schemaService, callback);
        assertFalse(task.isExcluded(LogicalDatastoreType.OPERATIONAL, QName.create("testA", REV1, "A")));

        // 'config' node at B@R1 is NOT excluded by [{"data-store": "config", "module-name": "A"}]
        task = new ExportTask(null, EXCL_CFG, true, false, getDomBroker(), schemaService, callback);
        assertFalse(task.isExcluded(LogicalDatastoreType.CONFIGURATION, QName.create("testB", REV1, "B")));

        // 'operational' node at B@R1 is NOT excluded by [{"data-store": "operational", "module-name": "A"}]
        task = new ExportTask(null, EXCL_OP, true, false, getDomBroker(), schemaService, callback);
        assertFalse(task.isExcluded(LogicalDatastoreType.OPERATIONAL, QName.create("testB", REV1, "B")));
    }
}
