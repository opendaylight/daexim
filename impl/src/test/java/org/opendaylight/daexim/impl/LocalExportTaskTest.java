/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.jayway.jsonpath.Configuration;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.binding.test.AbstractDataBrokerTest;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.core.api.model.SchemaService;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopologyBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NodeId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.TopologyId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.TpId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.TopologyBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.NodeBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.node.TerminationPointBuilder;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.model.api.SchemaContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalExportTaskTest extends AbstractDataBrokerTest {

    private static final Logger LOG = LoggerFactory.getLogger(LocalExportTaskTest.class);

    private SchemaContext schemaContext;
    private SchemaService schemaService;
    private Path tempDir;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("daexim-test-tmp");
        System.setProperty("karaf.home", tempDir.toString());
        LOG.info("Dump directory : {}", tempDir);
        super.setup();
    }

    @After
    public void tearDown() throws IOException {
        for (final File f : Arrays.asList(tempDir.resolve(Util.DAEXIM_DIR).toFile().listFiles())) {
            if (!f.isDirectory()) {
                LOG.info("Removing file : {}", f);
                Files.delete(f.toPath());
            }
        }
        Files.delete(tempDir.resolve(Util.DAEXIM_DIR));
        Files.delete(tempDir);
    }

    @Override
    protected void setupWithSchema(SchemaContext context) {
        this.schemaContext = context;
        super.setupWithSchema(context);
    }

    @Test
    public void test() throws Exception {
        schemaService = mock(SchemaService.class);
        when(schemaService.getGlobalContext()).thenReturn(schemaContext);
        final WriteTransaction wrTrx = getDataBroker().newWriteOnlyTransaction();
        final InstanceIdentifier<NetworkTopology> ii = InstanceIdentifier.create(NetworkTopology.class);
        final NetworkTopology dObj = new NetworkTopologyBuilder()
                .setTopology(Lists.newArrayList(new TopologyBuilder()
                        .setNode(Lists.newArrayList(
                                new NodeBuilder()
                                    .setNodeId(new NodeId("node-id-1"))
                                .build(),
                                new NodeBuilder()
                                    .setTerminationPoint(Lists.newArrayList(
                                            new TerminationPointBuilder()
                                                .setTpId(new TpId("eth0"))
                                            .build()
                                            ))
                                    .setNodeId(new NodeId("node-id-2"))
                            .build()
                                ))
                        .setTopologyId(new TopologyId("topo-id"))
                        .build()))
                .build();
        wrTrx.put(LogicalDatastoreType.OPERATIONAL, ii, dObj);
        wrTrx.submit().checkedGet();
        ExportTask lbt = new ExportTask(null, null, getDomBroker(), schemaService, mock(Callback.class));
        lbt.call();
        final String jsonStr = new String(Files.readAllBytes(Util.collectDataFiles()
                .get(LogicalDatastoreType.OPERATIONAL).get(0).toPath()), StandardCharsets.UTF_8);
        final Object json = Configuration.defaultConfiguration().jsonProvider().parse(jsonStr);
        assertThat(json,
                hasJsonPath("$.network-topology:network-topology.topology[0].topology-id", equalTo("topo-id")));
        assertThat(json, hasJsonPath("$.network-topology:network-topology.topology[0].node[*]", hasSize(2)));
        assertThat(json, hasJsonPath("$..termination-point[0].tp-id", contains("eth0")));
    }
}
