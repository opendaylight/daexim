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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.controller.md.sal.binding.api.ReadOnlyTransaction;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.binding.test.AbstractDataBrokerTest;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.ReadFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.controller.md.sal.dom.api.DOMDataReadOnlyTransaction;
import org.opendaylight.mdsal.dom.api.DOMSchemaService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.ImportOperationResult;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataStoreScope;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInputBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopologyBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NodeId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.TopologyBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Node;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.NodeBuilder;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier.PathArgument;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNode;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNodeContainer;
import org.opendaylight.yangtools.yang.model.api.SchemaContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportTaskTest extends AbstractDataBrokerTest {

    private static final Logger LOG = LoggerFactory.getLogger(ImportTaskTest.class);

    private static final String OLD_NODE_ID = "node-id-5";

    private DOMSchemaService schemaService;
    private SchemaContext schemaContext;
    private Path modelsFile;
    private Path opDataFile;
    private Path tmpDir;

    @Override
    protected void setupWithSchema(SchemaContext context) {
        this.schemaContext = context;
        super.setupWithSchema(context);
    }

    @Before
    public void setUp() throws IOException {
        tmpDir = Files.createTempDirectory("daexim-test-tmp");
        Files.createDirectory(tmpDir.resolve(Util.DAEXIM_DIR));
        System.setProperty("karaf.home", tmpDir.toString());
        LOG.info("Created temp directory : {}", tmpDir);

        modelsFile = Files.createFile(tmpDir.resolve(Util.DAEXIM_DIR).resolve(Util.FILE_PREFIX + "models.json"));
        opDataFile = Files.createFile(tmpDir.resolve(Util.DAEXIM_DIR).resolve(Util.FILE_PREFIX + "operational.json"));
        ByteStreams.copy(this.getClass().getResourceAsStream('/' + Util.FILE_PREFIX + "models.json"),
                Files.newOutputStream(modelsFile, StandardOpenOption.TRUNCATE_EXISTING));
        ByteStreams.copy(this.getClass().getResourceAsStream('/' + Util.FILE_PREFIX + "operational.json"),
                Files.newOutputStream(opDataFile, StandardOpenOption.TRUNCATE_EXISTING));

        LOG.info("Copied models file to  : {}", modelsFile);
        System.setProperty("java.io.tmpdir", tmpDir.toString());
        schemaService = mock(DOMSchemaService.class);
        doReturn(schemaContext).when(schemaService).getGlobalContext();
    }

    @After
    public void tearDown() throws IOException {
        removeFile(modelsFile);
        removeFile(opDataFile);
        Files.delete(tmpDir.resolve(Util.DAEXIM_DIR));
        Files.delete(tmpDir);
        reset(schemaService);
    }

    @SuppressWarnings("unchecked")
    private Collection<? extends NormalizedNode<?, ?>> readRoot() throws InterruptedException, ExecutionException {
        final DOMDataReadOnlyTransaction roTrx = getDomBroker().newReadOnlyTransaction();
        try {
            NormalizedNodeContainer<? extends PathArgument, ? extends PathArgument, ? extends NormalizedNode<?, ?>>
                nnc = (NormalizedNodeContainer<? extends PathArgument, ? extends PathArgument,
                        ? extends NormalizedNode<?, ?>>) roTrx.read(LogicalDatastoreType.OPERATIONAL,
                                YangInstanceIdentifier.EMPTY).get().get();
            return nnc.getValue();
        } finally {
            roTrx.close();
        }
    }

    private static void removeFile(Path path) throws IOException {
        if (path.toFile().exists()) {
            Files.delete(path);
        }
    }

    private ImportOperationResult runRestore(ImmediateImportInput input) throws Exception {
        final ImportTask rt = new ImportTask(
                new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All).setCheckModels(true)
                        .setStrictDataConsistency(true).build(),
                getDomBroker(), schemaService, false, mock(Callback.class));
        return rt.call();
    }

    private <D extends DataObject> void writeDataToRoot(InstanceIdentifier<D> ii, D dataObject)
            throws TransactionCommitFailedException {
        final WriteTransaction wrTrx = getDataBroker().newWriteOnlyTransaction();
        wrTrx.put(LogicalDatastoreType.OPERATIONAL, ii, dataObject);
        wrTrx.submit().checkedGet();
    }

    private <D extends DataObject> D readData(InstanceIdentifier<D> ii) throws ReadFailedException {
        try (ReadOnlyTransaction roTrx = getDataBroker().newReadOnlyTransaction()) {
            return roTrx.read(LogicalDatastoreType.OPERATIONAL, ii).checkedGet().get();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test() throws Exception {
        schemaService = mock(DOMSchemaService.class);
        doReturn(schemaContext).when(schemaService).getGlobalContext();
        final ImportTask rt = new ImportTask(
                new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All).setCheckModels(true)
                        .setStrictDataConsistency(true).build(),
                getDomBroker(), schemaService, false, mock(Callback.class));
        final ImportOperationResult result = rt.call();
        assertTrue(result.getReason(), result.isResult());
        Collection<? extends NormalizedNode<?, ?>> children = readRoot();
        assertEquals(1, children.size());
        NormalizedNode<?, ?> nn = children.iterator().next();
        assertEquals("network-topology", nn.getNodeType().getLocalName());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testImport_WithoutDataConsistency() throws Exception {
        schemaService = mock(DOMSchemaService.class);
        doReturn(schemaContext).when(schemaService).getGlobalContext();

        Collection<? extends NormalizedNode<?, ?>> childrenBefore = readRoot();
        assertEquals(0, childrenBefore.size());

        final ImportTask rt = new ImportTask(
                new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All).setCheckModels(true)
                        .setStrictDataConsistency(false).build(),
                getDomBroker(), schemaService, false, mock(Callback.class));
        final ImportOperationResult result = rt.call();

        assertTrue(result.getReason(), result.isResult());
        Collection<? extends NormalizedNode<?, ?>> childrenAfter = readRoot();
        assertEquals(1, childrenAfter.size());
        NormalizedNode<?, ?> nn = childrenAfter.iterator().next();
        assertEquals("network-topology", nn.getNodeType().getLocalName());
    }

    /**
     * Test case : {@link DataStoreScope} is ALL, but not data file is present.
     * <br />
     * expected behavior: any data in that datastore will be removed, but no new
     * data is imported
     */
    @Test
    public void testClearScopeALL_NoDatafile() throws Exception {
        ImportOperationResult result;
        // Remove data file first
        Files.delete(opDataFile);
        // Write arbitrary data to datastore
        writeDataToRoot(InstanceIdentifier.create(NetworkTopology.class), new NetworkTopologyBuilder()
                .setTopology(
                        Lists.newArrayList(new TopologyBuilder().setTopologyId(TestBackupData.TOPOLOGY_ID).build()))
                .build());
        // perform restore
        result = runRestore(new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All).setCheckModels(true)
                .setStrictDataConsistency(true).build());
        assertTrue(result.getReason(), result.isResult());
        // verify : no data present
        assertTrue(readRoot().isEmpty());
    }

    /**
     * Test case : {@link DataStoreScope} is Data, but not data file is present.
     * <br />
     * expected behavior: any data in that datastore will be removed, but no new
     * data is imported
     */
    @Test
    public void testClearScopeData_NoDatafile() throws Exception {
        ImportOperationResult result;
        // Remove data file first
        Files.delete(opDataFile);
        // Write arbitrary data to datastore
        writeDataToRoot(InstanceIdentifier.create(NetworkTopology.class), new NetworkTopologyBuilder()
                .setTopology(
                        Lists.newArrayList(new TopologyBuilder().setTopologyId(TestBackupData.TOPOLOGY_ID).build()))
                .build());
        // perform restore
        result = runRestore(new ImmediateImportInputBuilder().setClearStores(DataStoreScope.Data).setCheckModels(true)
                .setStrictDataConsistency(true).build());
        assertTrue(result.getReason(), result.isResult());
        // verify : no data present
        assertTrue(readRoot().isEmpty());
    }

    /**
     * Test case : DataStoreScope is NONE and data file is present. <br />
     * expected behavior : no data are removed prior to import - validate data
     * overwrote
     */
    @Test
    public void testClearScopeNone_OverWrote() throws Exception {
        // Write topology
        writeDataToRoot(InstanceIdentifier.create(NetworkTopology.class),
                new NetworkTopologyBuilder().setTopology(Lists.newArrayList(new TopologyBuilder()
                        .setNode(Lists.newArrayList(new NodeBuilder().setNodeId(new NodeId(OLD_NODE_ID)).build()))
                        .setTopologyId(TestBackupData.TOPOLOGY_ID).build())).build());
        // Perform restore with scope NONE
        ImportOperationResult result = runRestore(new ImmediateImportInputBuilder().setClearStores(DataStoreScope.None)
                .setCheckModels(true).setStrictDataConsistency(true).build());
        assertTrue(result.getReason(), result.isResult());
        // verify data get overwritten by restore (topology from JSON file has
        // same topology-id, but different set of nodes
        final Topology t = readData(TestBackupData.TOPOLOGY_II);
        assertEquals(2, t.getNode().size());
        for (final Node node : t.getNode()) {
            // make sure none of 'new' nodes has 'old' node id
            assertFalse(OLD_NODE_ID.equals(node.key().getNodeId().getValue()));
        }
    }
}
