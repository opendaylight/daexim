/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 * Copyright (c) 2017 Red Hat, Inc. and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ClassToInstanceMap;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opendaylight.daexim.spi.NodeNameProvider;
import org.opendaylight.infrautils.ready.testutils.TestSystemReadyMonitor;
import org.opendaylight.infrautils.ready.testutils.TestSystemReadyMonitor.Behaviour;
import org.opendaylight.infrautils.testutils.LogRule;
import org.opendaylight.mdsal.binding.api.ReadTransaction;
import org.opendaylight.mdsal.binding.api.RpcProviderService;
import org.opendaylight.mdsal.binding.dom.adapter.test.AbstractDataBrokerTest;
import org.opendaylight.mdsal.common.api.LogicalDatastoreType;
import org.opendaylight.mdsal.dom.api.DOMSchemaService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.AbsoluteTime;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.CancelExport;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.CancelExportInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataStoreScope;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImport;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.RelativeTime;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExport;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportInput.RunAt;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.Status;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusExport;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusExportInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusExportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusImport;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusImportInputBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
import org.opendaylight.yangtools.yang.binding.Rpc;
import org.opendaylight.yangtools.yang.common.ErrorType;
import org.opendaylight.yangtools.yang.common.RpcError;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.Uint32;
import org.opendaylight.yangtools.yang.model.api.EffectiveModelContext;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataExportImportAppProviderTest extends AbstractDataBrokerTest {

    private static final Logger LOG = LoggerFactory.getLogger(DataExportImportAppProviderTest.class);

    public @Rule LogRule logRule = new LogRule();

    private Path tempDir;
    private DataExportImportAppProvider provider;
    private NodeNameProvider nnp;
    private EffectiveModelContext schemaContext;
    private RpcProviderService rpcProvider;
    private ClassToInstanceMap<Rpc<?, ?>> rpcMap;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("daexim-test-tmp");
        System.setProperty("karaf.home", tempDir.toString());
        LOG.info("Dump directory : {}", tempDir);

        nnp = mock(NodeNameProvider.class);
        when(nnp.getNodeName()).thenReturn("localhost");

        schemaContext = getSchemaContext();
        // Do NOT initialize provider here; let each @Test do it;
        // that is because, in some tests, we want to do something before..
    }

    private void initProvider() {
        DOMSchemaService schemaService = mock(DOMSchemaService.class);
        when(schemaService.getGlobalContext()).thenReturn(schemaContext);
        rpcProvider = mock(RpcProviderService.class);
        provider = new DataExportImportAppProvider(getDataBroker(), getDomBroker(), schemaService, nnp, rpcProvider,
            new TestSystemReadyMonitor(Behaviour.IMMEDIATE), mock(BundleContext.class));
        rpcMap = provider.getRpcClassToInstanceMap();
    }

    @After
    public void tearDown() throws IOException {
        Files.setPosixFilePermissions(tempDir.resolve(Util.DAEXIM_DIR), Set.of(
                PosixFilePermission.OWNER_EXECUTE,PosixFilePermission.OWNER_READ,PosixFilePermission.OWNER_WRITE
                ));
        FileUtils.deleteDirectory(tempDir.toFile());
    }

    @Test(timeout = 15000)
    public void testExportStatusRPC() throws InterruptedException, ExecutionException {
        initProvider();
        final var result = rpcMap.getInstance(StatusExport.class).invoke(new StatusExportInputBuilder().build()).get();
        LOG.info("RPC result : {}", result);
        assertTrue(result.isSuccessful());
    }

    @Test(timeout = 15000)
    public void testImportStatusRPC() throws InterruptedException, ExecutionException {
        initProvider();
        final var result = rpcMap.getInstance(StatusImport.class).invoke(new StatusImportInputBuilder().build()).get();
        LOG.info("RPC result : {}", result);
        assertTrue(result.isSuccessful());
    }

    @Test(timeout = 15000)
    public void testScheduleMissingInfoRPC() throws InterruptedException, ExecutionException {
        initProvider();
        final var result = rpcMap.getInstance(ScheduleExport.class)
            .invoke(new ScheduleExportInputBuilder().build())
            .get();
        LOG.info("RPC result : {}", result);
        assertFalse(result.isSuccessful());
        RpcError err = result.getErrors().iterator().next();
        assertEquals(ErrorType.PROTOCOL, err.getErrorType());
    }

    @Test(timeout = 15000)
    public void testScheduleRelativeRPC() throws InterruptedException, ExecutionException {
        initProvider();
        final var result = rpcMap.getInstance(ScheduleExport.class)
            .invoke(new ScheduleExportInputBuilder().setRunAt(new RunAt(new RelativeTime(Uint32.TEN))).build())
            .get();
        LOG.info("RPC result : {}", result);
        assertTrue(result.isSuccessful());
        for (;;) {
            if (Status.Complete.equals(getStatus().getStatus())) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
    }

    @Test(timeout = 15000)
    public void testScheduleAbsoluteRPC() throws InterruptedException, ExecutionException {
        initProvider();
        final var result = rpcMap.getInstance(ScheduleExport.class)
            .invoke(new ScheduleExportInputBuilder()
                .setRunAt(new RunAt(new AbsoluteTime(Util.toDateAndTime(new Date(System.currentTimeMillis() + 5000)))))
                .build())
            .get();
        LOG.info("[Schedule absolute] RPC result : {}", result);
        assertTrue(result.toString(), result.isSuccessful());
        for (;;) {
            final StatusExportOutput s = getStatus();
            if (Status.Scheduled.equals(s.getStatus())) {
                assertNotNull(s.getRunAt());
                break;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
    }

    @Test(timeout = 15000)
    public void testCancelRPC() throws InterruptedException, ExecutionException {
        initProvider();
        final var result = rpcMap.getInstance(ScheduleExport.class)
            .invoke(new ScheduleExportInputBuilder().setRunAt(new RunAt(new RelativeTime(Uint32.valueOf(10000))))
                .build())
            .get();
        LOG.info("RPC result : {}", result);
        assertTrue(result.isSuccessful());
        // Wait for scheduled status
        for (;;) {
            if (Status.Scheduled.equals(getStatus().getStatus())) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        final var cancelResult = rpcMap.getInstance(CancelExport.class).invoke(new CancelExportInputBuilder().build())
            .get();
        LOG.info("RPC result : {}", cancelResult);
        assertTrue(cancelResult.isSuccessful());
        // wait for initial status after cancellation
        for (;;) {
            if (Status.Initial.equals(getStatus().getStatus())) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
    }

    @Test(timeout = 15000)
    public void testImportRPC() throws InterruptedException, ExecutionException, IOException {
        initProvider();
        final var result = rpcMap.getInstance(ScheduleExport.class)
            .invoke(new ScheduleExportInputBuilder()
                .setRunAt(new RunAt(new RunAt(new RelativeTime(Uint32.valueOf(500)))))
                .build())
            .get();
        LOG.info("RPC result : {}", result);
        assertTrue(result.isSuccessful());
        // Wait for completed status
        for (;;) {
            if (Status.Complete.equals(getStatus().getStatus())) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        final var rpcImmediate = rpcMap.getInstance(ImmediateImport.class);
        RpcResult<ImmediateImportOutput> importResult = rpcImmediate.invoke(new ImmediateImportInputBuilder()
                .setClearStores(DataStoreScope.All).setCheckModels(true).setStrictDataConsistency(true).build()).get();
        LOG.info("RPC result : {}", importResult);
        final var rpcStatus = ((StatusImport) rpcMap.get(StatusImport.class));
        assertEquals(Status.Complete,
            rpcStatus.invoke(new StatusImportInputBuilder().build()).get().getResult().getStatus());
        // Now, mess-up JSON file, so it fails
        final File f = Util.collectDataFiles(false).get(LogicalDatastoreType.OPERATIONAL).iterator().next();
        Files.writeString(f.toPath(), "some-garbage");
        importResult = rpcImmediate.invoke(new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All)
                .setCheckModels(true).setStrictDataConsistency(true).build()).get();
        LOG.info("RPC result : {}", importResult);
        assertEquals(Status.Failed,
                rpcStatus.invoke(new StatusImportInputBuilder().build()).get().getResult().getStatus());
    }

    /**
     * Tests the "auto-import-on-boot" feature.
     */
    @Test
    public void testImportOnBoot() throws Exception {
        // Given
        Resources.asByteSource(Resources.getResource("odl_backup_models.json"))
            .copyTo(com.google.common.io.Files.asByteSink(Util.getModelsFilePath(true).toFile()));
        File bootImportFile = Util.getDaeximFilePath(true, LogicalDatastoreType.OPERATIONAL).toFile();
        Resources.asByteSource(Resources.getResource("odl_backup_operational.json"))
            .copyTo(com.google.common.io.Files.asByteSink(bootImportFile));

        // When
        initProvider();
        provider.awaitBootImport("DataExportImportAppProviderTest.testImportOnBoot");

        // Then
        try (ReadTransaction tx = getDataBroker().newReadOnlyTransaction()) {
            Topology topo = tx.read(LogicalDatastoreType.OPERATIONAL, TestBackupData.TOPOLOGY_II).get().get();
            assertEquals(TestBackupData.TOPOLOGY_ID, topo.getTopologyId());
        }
        // Check that import-on-boot renamed processed file, to avoid continous re-import on every boot
        assertFalse(bootImportFile.exists());
    }

    @Test
    public void testImportOnBootWithNoFilesShouldNotBlockAwait() {
        // Given nothing to auto-import-on-boot, await should immediately return
        initProvider();
        provider.awaitBootImport("DataExportImportAppProviderTest.testImportOnBootWithNoFilesShouldNotBlockAwait");
    }

    @Test
    public void testImportOnBootWithBrokenJSON() throws Exception {
        // Given
        Resources.asByteSource(Resources.getResource("odl_backup_models.json"))
            .copyTo(com.google.common.io.Files.asByteSink(Util.getModelsFilePath(true).toFile()));
        File bootImportFile = Util.getDaeximFilePath(true, LogicalDatastoreType.OPERATIONAL).toFile();
        Files.writeString(bootImportFile.toPath(), "some-garbage");

        // When
        initProvider();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> provider.awaitBootImport("DataExportImportAppProviderTest.testImportOnBootWithBrokenJSON"));
        assertEquals("Node (urn:ietf:params:xml:ns:netconf:base:1.0)data is not a simple type", ex.getMessage());

        // Check that even a failed import-on-boot renamed processed file,
        // to avoid continuous re-import on every boot in case of failures
        assertFalse(bootImportFile.exists());
    }

    @Test
    public void testImportOnBootWithMissingModelsFile() throws Exception {
        // Given
        File bootImportFile = Util.getDaeximFilePath(true, LogicalDatastoreType.OPERATIONAL).toFile();
        Resources.asByteSource(Resources.getResource("odl_backup_operational.json"))
            .copyTo(com.google.common.io.Files.asByteSink(bootImportFile));

        // When
        initProvider();
        provider.awaitBootImport("DataExportImportAppProviderTest.testImportOnBoot");

        // Then
        try (ReadTransaction tx = getDataBroker().newReadOnlyTransaction()) {
            Topology topo = tx.read(LogicalDatastoreType.OPERATIONAL, TestBackupData.TOPOLOGY_II).get().get();
            assertEquals(TestBackupData.TOPOLOGY_ID, topo.getTopologyId());
        }

        // Check that import-on-boot renamed processed file,
        // to avoid continuous re-import on every boot in case of failures
        assertFalse(bootImportFile.exists());
    }

    @Test
    public void testExportWithPermissionDenied() throws IOException, InterruptedException, ExecutionException {
        initProvider();
        Files.setPosixFilePermissions(tempDir.resolve(Util.DAEXIM_DIR), Set.of());
        final var result = rpcMap.getInstance(ScheduleExport.class)
            .invoke(new ScheduleExportInputBuilder().setRunAt(new RunAt(new RelativeTime(Uint32.TEN))).build())
            .get();
        LOG.info("RPC result : {}", result);
        assertTrue(result.isSuccessful());
        for (;;) {
            final StatusExportOutput s = getStatus();
            if (Status.Failed.equals(s.getStatus())) {
                assertNotNull(s.nonnullNodes().values().iterator().next().getReason());
                break;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
    }

    private StatusExportOutput getStatus() throws InterruptedException, ExecutionException {
        return rpcMap.getInstance(StatusExport.class).invoke(new StatusExportInputBuilder().build()).get().getResult();
    }
}
