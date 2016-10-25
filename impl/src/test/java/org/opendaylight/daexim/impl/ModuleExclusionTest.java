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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.controller.md.sal.binding.test.AbstractDataBrokerTest;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.controller.sal.core.api.model.SchemaService;
import org.opendaylight.daexim.impl.Callback;
import org.opendaylight.daexim.impl.ExportTask;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev130715.YangIdentifier;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataStore;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModules;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModules.ModuleName;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModulesBuilder;
import org.opendaylight.yangtools.yang.common.QName;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier.PathArgument;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNode;
import org.opendaylight.yangtools.yang.model.api.SchemaContext;

import com.google.common.collect.ImmutableList;

public class ModuleExclusionTest extends AbstractDataBrokerTest {
    private static final String REV1 = "2016-09-13";

    private static final List<ExcludedModules> EXCL_CFG = ImmutableList.<ExcludedModules>builder()
            .add(new ExcludedModulesBuilder().setDataStore(new DataStore("config"))
                    .setModuleName(new ModuleName(new YangIdentifier("A")))
            .build()).build();

    private static final List<ExcludedModules> EXCL_OP = ImmutableList.<ExcludedModules>builder()
            .add(new ExcludedModulesBuilder().setDataStore(new DataStore("operational"))
                    .setModuleName(new ModuleName(new YangIdentifier("A")))
            .build()).build();

    private static final String REV2 = "2016-09-12";

    private SchemaContext schemaContext;
    private SchemaService schemaService;

    @Before
    public void setUp() throws TransactionCommitFailedException {
        schemaService = mock(SchemaService.class);
        when(schemaService.getGlobalContext()).thenReturn(schemaContext);
    }

    @After
    public void tearDown() {
        reset(schemaService);
    }

    @Override
    protected void setupWithSchema(SchemaContext context) {
        this.schemaContext = context;
        super.setupWithSchema(context);
    }

    @Test
    public void test() throws Exception {
        ExportTask t;
        // 'config' node at A@R1 is excluded by [{"data-store": "config", "module-name": "A"}]
        t = new ExportTask(EXCL_CFG, getDomBroker(), schemaService, mock(Callback.class));
        assertTrue(t.isExcluded(LogicalDatastoreType.CONFIGURATION, constructNode("testA", REV1, "A")));

        // 'operational' node at A@R1 is excluded by [{"data-store": "operational", "module-name": "A"}]
        t = new ExportTask(EXCL_OP, getDomBroker(), schemaService, mock(Callback.class));
        assertTrue(t.isExcluded(LogicalDatastoreType.OPERATIONAL, constructNode("testA", REV1, "A")));

        // 'config' node at A@R2 is excluded by [{"data-store": "config", "module-name": "A"}]
        t = new ExportTask(EXCL_CFG, getDomBroker(), schemaService, mock(Callback.class));
        assertTrue(t.isExcluded(LogicalDatastoreType.CONFIGURATION, constructNode("testA", REV2, "A")));

        // 'operational' node at A@R1 is excluded by [{"data-store": "operational", "module-name": "A"}]
        t = new ExportTask(EXCL_OP, getDomBroker(), schemaService, mock(Callback.class));
        assertTrue(t.isExcluded(LogicalDatastoreType.OPERATIONAL, constructNode("testA", REV1, "A")));

        // 'config' node at A@R1 is NOT excluded by [{"data-store": "operational", "module-name": "A"}]
        t = new ExportTask(EXCL_OP, getDomBroker(), schemaService, mock(Callback.class));
        assertFalse(t.isExcluded(LogicalDatastoreType.CONFIGURATION, constructNode("testA", REV1, "A")));

        //'operational' node at A@R1 is NOT excluded by [{"data-store": "config", "module-name": "A"}]
        t = new ExportTask(EXCL_CFG, getDomBroker(), schemaService, mock(Callback.class));
        assertFalse(t.isExcluded(LogicalDatastoreType.OPERATIONAL, constructNode("testA", REV1, "A")));

        // 'config' node at B@R1 is NOT excluded by [{"data-store": "config", "module-name": "A"}]
        t = new ExportTask(EXCL_CFG, getDomBroker(), schemaService, mock(Callback.class));
        assertFalse(t.isExcluded(LogicalDatastoreType.CONFIGURATION, constructNode("testB", REV1, "B")));

        // 'operational' node at B@R1 is NOT excluded by [{"data-store": "operational", "module-name": "A"}]
        t = new ExportTask(EXCL_OP, getDomBroker(), schemaService, mock(Callback.class));
        assertFalse(t.isExcluded(LogicalDatastoreType.OPERATIONAL, constructNode("testB", REV1, "B")));
    }

    private NormalizedNode<?, ?> constructNode(String namespace, String revision, String module) {
        @SuppressWarnings("unchecked")
        final NormalizedNode<PathArgument, ?> nn = mock(NormalizedNode.class);
        PathArgument pa = mock(PathArgument.class);
        doReturn(QName.create(namespace, revision, module)).when(pa).getNodeType();
        doReturn(pa).when(nn).getIdentifier();
        return nn;
    }
}
