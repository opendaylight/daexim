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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opendaylight.controller.md.sal.binding.test.AbstractDataBrokerTest;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.RoutedRpcRegistration;
import org.opendaylight.controller.sal.binding.api.RpcProviderRegistry;
import org.opendaylight.controller.sal.core.api.model.SchemaService;
import org.opendaylight.daexim.impl.DataExportImportAppProvider;
import org.opendaylight.daexim.impl.Util;
import org.opendaylight.daexim.spi.NodeNameProvider;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.AbsoluteTime;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.CancelExportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataExportImportService;
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
import org.opendaylight.yangtools.yang.common.RpcError;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.model.api.SchemaContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;

public class DataExportImportAppProviderTest extends AbstractDataBrokerTest {
    private static final Logger LOG = LoggerFactory.getLogger(DataExportImportAppProviderTest.class);
    private static Path tempDir;
    private DataExportImportAppProvider provider;
    private SchemaContext schemaContext;
    private NodeNameProvider nnp;
    private RpcProviderRegistry rpcProviderRegistry;

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        tempDir = Files.createTempDirectory("daexim-test-tmp");
        System.setProperty("karaf.home", tempDir.toString());
        LOG.info("Dump directory : {}", tempDir);
    }

    @AfterClass
    public static void tearDownAfterClass() throws IOException {
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
        super.setupWithSchema(context);
        this.schemaContext = context;
    }

    @Before
    public void setUp() throws Exception {
        rpcProviderRegistry = mock(RpcProviderRegistry.class);
        nnp = mock(NodeNameProvider.class);
        when(nnp.getNodeName()).thenReturn("localhost");
        provider = new DataExportImportAppProvider();
        doReturn(mock(RoutedRpcRegistration.class)).when(rpcProviderRegistry)
                .addRpcImplementation(eq(DataExportImportService.class), eq(provider));
        provider.setDataBroker(getDataBroker());
        provider.setDomDataBroker(getDomBroker());
        SchemaService schemaService = mock(SchemaService.class);
        when(schemaService.getGlobalContext()).thenReturn(schemaContext);
        provider.setSchemaService(schemaService);
        provider.setRpcProviderRegistry(rpcProviderRegistry);
        provider.setNodeNameProvider(nnp);
        provider.init();
    }

    @After
    public void tearDown() throws IOException {
        Files.setPosixFilePermissions(tempDir.resolve(Util.DAEXIM_DIR), Sets.<PosixFilePermission>newHashSet(
                PosixFilePermission.OWNER_EXECUTE,PosixFilePermission.OWNER_READ,PosixFilePermission.OWNER_WRITE
                ));
        provider.close();
    }

    @Test(timeout = 15000)
    public void testExportStatusRPC() throws InterruptedException, ExecutionException {
        final RpcResult<StatusExportOutput> result = provider.statusExport().get();
        LOG.info("RPC result : {}", result);
        assertTrue(result.isSuccessful());
    }

    @Test(timeout = 15000)
    public void testImportStatusRPC() throws InterruptedException, ExecutionException {
        final RpcResult<StatusImportOutput> result = provider.statusImport().get();
        LOG.info("RPC result : {}", result);
        assertTrue(result.isSuccessful());
    }

    @Test(timeout = 15000)
    public void testScheduleMissingInfoRPC() throws InterruptedException, ExecutionException {
        final RpcResult<ScheduleExportOutput> result = provider.scheduleExport(new ScheduleExportInputBuilder().build())
                .get();
        LOG.info("RPC result : {}", result);
        assertFalse(result.isSuccessful());
        RpcError err = result.getErrors().iterator().next();
        assertEquals(RpcError.ErrorType.PROTOCOL, err.getErrorType());
    }

    @Test(timeout = 15000)
    public void testScheduleRelativeRPC() throws InterruptedException, ExecutionException {
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
        final RpcResult<ScheduleExportOutput> result = provider.scheduleExport(new ScheduleExportInputBuilder()
                .setRunAt(new RunAt(new AbsoluteTime(Util.toDateAndTime(new Date(System.currentTimeMillis() + 5000)))))
                .build()).get();
        LOG.info("[Schedule absolute] RPC result : {}", result);
        assertTrue(result.isSuccessful());
        for (;;) {
            final StatusExportOutput s = getStatus();
            if(Status.Scheduled.equals(s.getStatus())) {
                assertNotNull(s.getRunAt());
                break;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
    }

    @Test(timeout = 15000)
    public void testCancelRPC() throws InterruptedException, ExecutionException {
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
        // Now, mess-up JSON file, so it fail
        final File f = Util.collectDataFiles().get(LogicalDatastoreType.OPERATIONAL).iterator().next();
        Files.write(f.toPath(), "some-garbage".getBytes(Charsets.UTF_8));
        importResult = provider.immediateImport(
                new ImmediateImportInputBuilder().setClearStores(DataStoreScope.All).setCheckModels(true).build())
                .get();
        LOG.info("RPC result : {}", importResult);
        assertEquals(Status.Failed, provider.statusImport().get().getResult().getStatus());
    }

    @Test
    public void testExportWithPermissionDenied() throws IOException, InterruptedException, ExecutionException {
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
