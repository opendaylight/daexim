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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType.OPERATIONAL;

import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.controller.md.sal.binding.api.ReadOnlyTransaction;
import org.opendaylight.controller.md.sal.binding.test.AbstractDataBrokerTest;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.core.api.model.SchemaService;
import org.opendaylight.daexim.spi.NodeNameProvider;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.AbsoluteTime;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.CancelExportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataStoreScope;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.RelativeTime;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportInput.RunAt;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.Status;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusExportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusImportOutput;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
import org.opendaylight.yangtools.yang.common.RpcError;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.model.api.SchemaContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataExportImportAppProviderTest extends AbstractDataBrokerTest {

    private static final Logger LOG = LoggerFactory.getLogger(DataExportImportAppProviderTest.class);

    private Path tempDir;
    private DataExportImportAppProvider provider;
    private SchemaContext schemaContext;
    private NodeNameProvider nnp;

    @Before
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("daexim-test-tmp");
        System.setProperty("karaf.home", tempDir.toString());
        LOG.info("Dump directory : {}", tempDir);

        nnp = mock(NodeNameProvider.class);
        when(nnp.getNodeName()).thenReturn("localhost");
        SchemaService schemaService = mock(SchemaService.class);
        when(schemaService.getGlobalContext()).thenReturn(schemaContext);
        provider = new DataExportImportAppProvider(getDataBroker(), getDomBroker(), schemaService, nnp);
        // Do NOT provider.init(); just yet; let each @Test do it;
        // that is because, in some tests, we want to do something before..
    }

    @After
    public void tearDown() throws IOException {
        Files.setPosixFilePermissions(tempDir.resolve(Util.DAEXIM_DIR), Sets.<PosixFilePermission>newHashSet(
                PosixFilePermission.OWNER_EXECUTE,PosixFilePermission.OWNER_READ,PosixFilePermission.OWNER_WRITE
                ));
        FileUtils.deleteDirectory(tempDir.toFile());
        provider.close();
    }

    @Override
    protected void setupWithSchema(SchemaContext context) {
        super.setupWithSchema(context);
        this.schemaContext = context;
    }

    @Test(timeout = 15000)
    public void testExportStatusRPC() throws InterruptedException, ExecutionException {
        provider.init();
        final RpcResult<StatusExportOutput> result = provider.statusExport().get();
        LOG.info("RPC result : {}", result);
        assertTrue(result.isSuccessful());
    }

    @Test(timeout = 15000)
    public void testImportStatusRPC() throws InterruptedException, ExecutionException {
        provider.init();
        final RpcResult<StatusImportOutput> result = provider.statusImport().get();
        LOG.info("RPC result : {}", result);
        assertTrue(result.isSuccessful());
    }

    @Test(timeout = 15000)
    public void testScheduleMissingInfoRPC() throws InterruptedException, ExecutionException {
        provider.init();
        final RpcResult<ScheduleExportOutput> result = provider.scheduleExport(new ScheduleExportInputBuilder().build())
                .get();
        LOG.info("RPC result : {}", result);
        assertFalse(result.isSuccessful());
        RpcError err = result.getErrors().iterator().next();
        assertEquals(RpcError.ErrorType.PROTOCOL, err.getErrorType());
    }

    @Test(timeout = 15000)
    public void testScheduleRelativeRPC() throws InterruptedException, ExecutionException {
        provider.init();
        final RpcResult<ScheduleExportOutput> result = provider
                .scheduleExport(new ScheduleExportInputBuilder().setRunAt(new RunAt(new RelativeTime(10L))).build())
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
        provider.init();
        final RpcResult<ScheduleExportOutput> result = provider.scheduleExport(new ScheduleExportInputBuilder()
                .setRunAt(new RunAt(new AbsoluteTime(Util.toDateAndTime(new Date(System.currentTimeMillis() + 5000)))))
                .build()).get();
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
        provider.init();
        final RpcResult<ScheduleExportOutput> result = provider
                .scheduleExport(new ScheduleExportInputBuilder().setRunAt(new RunAt(new RelativeTime(10000L))).build())
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
        final RpcResult<CancelExportOutput> cancelResult = provider.cancelExport().get();
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
        provider.init();
        final RpcResult<ScheduleExportOutput> result = provider
                .scheduleExport(
                        new ScheduleExportInputBuilder().setRunAt(new RunAt(new RunAt(new RelativeTime(500L)))).build())
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
        RpcResult<ImmediateImportOutput> importResult = provider.immediateImport(
                new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All).setCheckModels(true).build())
                .get();
        LOG.info("RPC result : {}", importResult);
        assertEquals(Status.Complete, provider.statusImport().get().getResult().getStatus());
        // Now, mess-up JSON file, so it fails
        final File f = Util.collectDataFiles(false).get(LogicalDatastoreType.OPERATIONAL).iterator().next();
        Files.write(f.toPath(), "some-garbage".getBytes(StandardCharsets.UTF_8));
        importResult = provider.immediateImport(
                new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All).setCheckModels(true).build())
                .get();
        LOG.info("RPC result : {}", importResult);
        assertEquals(Status.Failed, provider.statusImport().get().getResult().getStatus());
    }

    /**
     * Tests the "auto-import-on-boot" feature.
     */
    @Test
    public void testImportOnBoot() throws Exception {
        // Given
        Resources.asByteSource(Resources.getResource("odl_backup_models.json"))
            .copyTo(com.google.common.io.Files.asByteSink(Util.getModelsFilePath(true).toFile()));
        File bootImportFile = Util.getDaeximFilePath(true, OPERATIONAL).toFile();
        Resources.asByteSource(Resources.getResource("odl_backup_operational.json"))
            .copyTo(com.google.common.io.Files.asByteSink(bootImportFile));

        // When
        provider.init();
        provider.awaitBootImport("DataExportImportAppProviderTest.testImportOnBoot");

        // Then
        try (ReadOnlyTransaction tx = getDataBroker().newReadOnlyTransaction()) {
            Topology topo = tx.read(OPERATIONAL, TestBackupData.TOPOLOGY_II).get().get();
            assertEquals(TestBackupData.TOPOLGY_ID, topo.getTopologyId());
        }
        // Check that import-on-boot renamed processed file, to avoid continous re-import on every boot
        assertFalse(bootImportFile.exists());
    }

    @Test
    public void testImportOnBootWithNoFilesShouldNotBlockAwait() {
        // Given nothing to auto-import-on-boot, await should immediately return
        provider.init();
        provider.awaitBootImport("DataExportImportAppProviderTest.testImportOnBootWithNoFilesShouldNotBlockAwait");
    }

    @Test
    public void testImportOnBootWithBrokenJSON() throws Exception {
        // Given
        Resources.asByteSource(Resources.getResource("odl_backup_models.json"))
            .copyTo(com.google.common.io.Files.asByteSink(Util.getModelsFilePath(true).toFile()));
        File bootImportFile = Util.getDaeximFilePath(true, OPERATIONAL).toFile();
        Files.write(bootImportFile.toPath(), "some-garbage".getBytes(StandardCharsets.UTF_8));

        // When
        provider.init();
        try {
            provider.awaitBootImport("DataExportImportAppProviderTest.testImportOnBootWithBrokenJSON");
            fail("expected IllegalStateException");
        } catch (IllegalStateException e) {
            // OK, that's the expected outcome
        }

        // Check that even a failed import-on-boot renamed processed file,
        // to avoid continuous re-import on every boot in case of failures
        assertFalse(bootImportFile.exists());
    }

    @Test
    public void testImportOnBootWithMissingModelsFile() throws Exception {
        // Given
        File bootImportFile = Util.getDaeximFilePath(true, OPERATIONAL).toFile();
        Resources.asByteSource(Resources.getResource("odl_backup_operational.json"))
            .copyTo(com.google.common.io.Files.asByteSink(bootImportFile));

        // When
        provider.init();
        try {
            provider.awaitBootImport("DataExportImportAppProviderTest.testImportOnBootWithMissingModelsFile");
            fail("expected IllegalStateException");
        } catch (IllegalStateException e) {
            // OK, that's the expected outcome
        }

        // Check that import-on-boot renamed processed file,
        // to avoid continuous re-import on every boot in case of failures
        assertFalse(bootImportFile.exists());
    }

    @Test
    public void testExportWithPermissionDenied() throws IOException, InterruptedException, ExecutionException {
        provider.init();
        Files.setPosixFilePermissions(tempDir.resolve(Util.DAEXIM_DIR), Sets.<PosixFilePermission>newHashSet());
        final RpcResult<ScheduleExportOutput> result = provider
                .scheduleExport(new ScheduleExportInputBuilder().setRunAt(new RunAt(new RelativeTime(10L))).build())
                .get();
        LOG.info("RPC result : {}", result);
        assertTrue(result.isSuccessful());
        for (;;) {
            final StatusExportOutput s = getStatus();
            if (Status.Failed.equals(s.getStatus())) {
                assertNotNull(s.getNodes().iterator().next().getReason());
                break;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
    }

    private StatusExportOutput getStatus() throws InterruptedException, ExecutionException {
        return provider.statusExport().get().getResult();
    }
}
