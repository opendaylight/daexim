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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.mdsal.binding.api.ReadTransaction;
import org.opendaylight.mdsal.binding.api.WriteTransaction;
import org.opendaylight.mdsal.binding.dom.adapter.test.AbstractDataBrokerTest;
import org.opendaylight.mdsal.common.api.LogicalDatastoreType;
import org.opendaylight.mdsal.common.api.TransactionCommitFailedException;
import org.opendaylight.mdsal.dom.api.DOMDataTreeReadTransaction;
import org.opendaylight.mdsal.dom.api.DOMSchemaService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.ImportOperationResult;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataStoreScope;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.immediate._import.input.ImportBatchingBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopologyBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NodeId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.TopologyBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Node;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.NodeBuilder;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.util.BindingMap;
import org.opendaylight.yangtools.yang.common.Uint16;
import org.opendaylight.yangtools.yang.common.Uint32;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier.PathArgument;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNode;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNodeContainer;
import org.opendaylight.yangtools.yang.model.api.EffectiveModelContext;
import org.opendaylight.yangtools.yang.model.api.SchemaContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportTaskTest extends AbstractDataBrokerTest {
    private static final Logger LOG = LoggerFactory.getLogger(ImportTaskTest.class);
    private static final String OLD_NODE_ID = "node-id-5";
    @SuppressWarnings("unchecked")
    private final Consumer<Void> callback = mock(Consumer.class);
    private DOMSchemaService schemaService;
    private SchemaContext schemaContext;
    private Path modelsFile;
    private Path opDataFile;
    private Path tmpDir;

    @Override
    protected void setupWithSchema(EffectiveModelContext context) {
        this.schemaContext = context;
        super.setupWithSchema(context);
    }

    @Before
    public void setUp() throws Exception {
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
        final DOMDataTreeReadTransaction roTrx = getDomBroker().newReadOnlyTransaction();
        try {
            NormalizedNodeContainer<? extends PathArgument, ? extends PathArgument, ? extends NormalizedNode<?, ?>>
                nnc = (NormalizedNodeContainer<? extends PathArgument, ? extends PathArgument,
                        ? extends NormalizedNode<?, ?>>) roTrx.read(LogicalDatastoreType.OPERATIONAL,
                                YangInstanceIdentifier.empty()).get().get();
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
        final ImportTask rt = new ImportTask(input, getDomBroker(), schemaService, false, callback);
        return rt.call();
    }

    private <D extends DataObject> void writeDataToRoot(InstanceIdentifier<D> ii, D dataObject)
            throws TransactionCommitFailedException, InterruptedException, ExecutionException {
        final WriteTransaction wrTrx = getDataBroker().newWriteOnlyTransaction();
        wrTrx.put(LogicalDatastoreType.OPERATIONAL, ii, dataObject);
        wrTrx.commit().get();
    }

    private <D extends DataObject> D readData(InstanceIdentifier<D> ii)
            throws InterruptedException, ExecutionException {
        try (ReadTransaction roTrx = getDataBroker().newReadOnlyTransaction()) {
            return roTrx.read(LogicalDatastoreType.OPERATIONAL, ii).get().get();
        }
    }

    private void verifyNetworkTopologyInDataStore() throws InterruptedException, ExecutionException {
        final NetworkTopology nt = readData(InstanceIdentifier.create(NetworkTopology.class));
        assertNotNull(nt.getTopology());
        assertEquals(2, nt.getTopology().size());
        for (final Topology t : nt.nonnullTopology().values()) {
            assertNotNull(t.getNode());
            if (TestBackupData.TOPOLOGY_ID.equals(t.getTopologyId())) {
                assertEquals(2, t.getNode().size());
            } else if (TestBackupData.TOPOLOGY_ID_2.equals(t.getTopologyId())) {
                assertEquals(3, t.getNode().size());
            }
        }
    }

    @Test
    public void test() throws Exception {
        schemaService = mock(DOMSchemaService.class);
        doReturn(schemaContext).when(schemaService).getGlobalContext();
        final ImportTask rt = new ImportTask(
                new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All).setCheckModels(true)
                        .setStrictDataConsistency(true).build(),
                getDomBroker(), schemaService, false, callback);
        final ImportOperationResult result = rt.call();
        assertTrue(result.getReason(), result.getResult());
        Collection<? extends NormalizedNode<?, ?>> children = readRoot();
        assertEquals(1, children.size());
        NormalizedNode<?, ?> nn = children.iterator().next();
        assertEquals("network-topology", nn.getNodeType().getLocalName());
    }

    @Test
    public void testImport_WithoutDataConsistency() throws Exception {
        schemaService = mock(DOMSchemaService.class);
        doReturn(schemaContext).when(schemaService).getGlobalContext();

        Collection<? extends NormalizedNode<?, ?>> childrenBefore = readRoot();
        assertEquals(0, childrenBefore.size());

        final ImportTask rt = new ImportTask(
                new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All).setCheckModels(true)
                        .setStrictDataConsistency(false).build(),
                getDomBroker(), schemaService, false, callback);
        final ImportOperationResult result = rt.call();
        assertTrue(result.getReason(), result.getResult());
        verifyNetworkTopologyInDataStore();
        assertEquals(rt.getBatchCount(), 3);
        assertEquals(rt.getWriteCount(), 1);
    }

    @Test
    public void testImport_WithBatchingLevel2Size1() throws Exception {
        Collection<? extends NormalizedNode<?, ?>> childrenBefore = readRoot();
        assertEquals(0, childrenBefore.size());

        final ImportTask rt = new ImportTask(
                new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All).setCheckModels(true)
                        .setStrictDataConsistency(false)
                        .setImportBatching(new ImportBatchingBuilder().setMaxTraversalDepth(Uint16.valueOf(2))
                                .setListBatchSize(Uint32.ONE).build())
                        .build(),
                getDomBroker(), schemaService, false, callback);
        final ImportOperationResult result = rt.call();
        assertTrue(result.getReason(), result.getResult());
        verifyNetworkTopologyInDataStore();
        assertEquals(rt.getBatchCount(), 6);
        assertEquals(rt.getWriteCount(), 3);
    }

    public void testImport_WithBatchingLevel4Size2() throws Exception {
        Collection<? extends NormalizedNode<?, ?>> childrenBefore = readRoot();
        assertEquals(0, childrenBefore.size());

        final ImportTask rt = new ImportTask(
                new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All).setCheckModels(true)
                        .setStrictDataConsistency(false)
                        .setImportBatching(new ImportBatchingBuilder().setMaxTraversalDepth(Uint16.valueOf(4))
                                .setListBatchSize(Uint32.TWO).build())
                        .build(),
                getDomBroker(), schemaService, false, callback);
        final ImportOperationResult result = rt.call();
        assertTrue(result.getReason(), result.getResult());
        verifyNetworkTopologyInDataStore();
        assertEquals(rt.getBatchCount(), 6);
        assertEquals(rt.getWriteCount(), 5);
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
                .setTopology(BindingMap.of(new TopologyBuilder().setTopologyId(TestBackupData.TOPOLOGY_ID).build()))
                .build());
        // perform restore
        result = runRestore(new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All).setCheckModels(true)
                .setStrictDataConsistency(true).build());
        assertTrue(result.getReason(), result.getResult());
        // verify : no data present
        assertTrue(readRoot().isEmpty());
    }

    /**
     * Test case : {@link DataStoreScope} is Data, but no data file is present.
     * <br />
     * expected behavior: any data in that datastore will be retained, but no new
     * data is imported
     */
    @Test
    public void testClearScopeData_NoDatafile() throws Exception {
        ImportOperationResult result;
        // Remove data file first
        Files.delete(opDataFile);
        // Write arbitrary data to datastore
        writeDataToRoot(InstanceIdentifier.create(NetworkTopology.class), new NetworkTopologyBuilder()
                .setTopology(BindingMap.of(new TopologyBuilder().setTopologyId(TestBackupData.TOPOLOGY_ID).build()))
                .build());
        // perform restore
        result = runRestore(new ImmediateImportInputBuilder().setClearStores(DataStoreScope.Data).setCheckModels(true)
                .setStrictDataConsistency(true).build());
        assertTrue(result.getReason(), result.getResult());
        // verify : no data present
        assertFalse(readRoot().isEmpty());
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
                new NetworkTopologyBuilder().setTopology(BindingMap.of(new TopologyBuilder()
                        .setNode(BindingMap.of(new NodeBuilder().setNodeId(new NodeId(OLD_NODE_ID)).build()))
                        .setTopologyId(TestBackupData.TOPOLOGY_ID).build())).build());
        // Perform restore with scope NONE
        ImportOperationResult result = runRestore(new ImmediateImportInputBuilder().setClearStores(DataStoreScope.None)
                .setCheckModels(true).setStrictDataConsistency(true).build());
        assertTrue(result.getReason(), result.getResult());
        // verify data get overwritten by restore (topology from JSON file has
        // same topology-id, but different set of nodes
        final Topology t = readData(TestBackupData.TOPOLOGY_II);
        assertEquals(2, t.getNode().size());
        for (final Node node : t.nonnullNode().values()) {
            // make sure none of 'new' nodes has 'old' node id
            assertFalse(OLD_NODE_ID.equals(node.key().getNodeId().getValue()));
        }
    }

    @Test
    public void testWithFilterNonExistent() throws Exception {
        ImportOperationResult result = runRestore(new ImmediateImportInputBuilder().setClearStores(DataStoreScope.None)
                .setCheckModels(true).setFileNameFilter("non-existent")
                .setStrictDataConsistency(true).build());
        assertTrue(result.getReason(), result.getResult());

        Collection<? extends NormalizedNode<?, ?>> childrenAfter = readRoot();
        assertEquals(0,childrenAfter.size());
    }

    @Test
    public void testWithFilter() throws Exception {
        ImportOperationResult result = runRestore(new ImmediateImportInputBuilder().setClearStores(DataStoreScope.None)
                .setCheckModels(true).setFileNameFilter("odl_backup_operational.*")
                .setStrictDataConsistency(true).build());
        assertTrue(result.getReason(), result.getResult());

        Collection<? extends NormalizedNode<?, ?>> childrenAfter = readRoot();
        assertEquals(1,childrenAfter.size());
    }
}
