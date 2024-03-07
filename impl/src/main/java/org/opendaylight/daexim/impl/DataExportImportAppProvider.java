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

import static org.opendaylight.mdsal.common.api.LogicalDatastoreType.CONFIGURATION;
import static org.opendaylight.mdsal.common.api.LogicalDatastoreType.OPERATIONAL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableClassToInstanceMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.eclipse.jdt.annotation.Nullable;
import org.opendaylight.daexim.DataImportBootReady;
import org.opendaylight.daexim.DataImportBootService;
import org.opendaylight.daexim.spi.NodeNameProvider;
import org.opendaylight.infrautils.ready.SystemReadyMonitor;
import org.opendaylight.infrautils.utils.concurrent.ThreadFactoryProvider;
import org.opendaylight.mdsal.binding.api.DataBroker;
import org.opendaylight.mdsal.binding.api.DataTreeIdentifier;
import org.opendaylight.mdsal.binding.api.DataTreeModification;
import org.opendaylight.mdsal.binding.api.ReadTransaction;
import org.opendaylight.mdsal.binding.api.RpcProviderService;
import org.opendaylight.mdsal.binding.api.WriteTransaction;
import org.opendaylight.mdsal.common.api.LogicalDatastoreType;
import org.opendaylight.mdsal.dom.api.DOMDataBroker;
import org.opendaylight.mdsal.dom.api.DOMSchemaService;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev130715.DateAndTime;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.Daexim;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.ImportOperationResult;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.IpcType;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.daexim.DaeximControl;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.daexim.DaeximControlBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.daexim.DaeximStatus;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.daexim.DaeximStatusBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.daexim.daexim.status.NodeStatus;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.daexim.daexim.status.NodeStatusBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.daexim.daexim.status.NodeStatusKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.AbsoluteTime;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.CancelExport;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.CancelExportInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.CancelExportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.CancelExportOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataStoreScope;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImport;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.OperationStatus;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExport;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportInput.RunAt;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.Status;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusExport;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusExportInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusExportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusExportOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusImport;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusImportInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusImportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusImportOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status.export.output.Nodes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status.export.output.NodesBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status.export.output.NodesKey;
import org.opendaylight.yangtools.concepts.Registration;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.Rpc;
import org.opendaylight.yangtools.yang.binding.util.BindingMap;
import org.opendaylight.yangtools.yang.common.ErrorType;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.RequireServiceComponentRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Component(service = {})
@RequireServiceComponentRuntime
public class DataExportImportAppProvider implements DataImportBootService, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DataExportImportAppProvider.class);
    private static final InstanceIdentifier<Daexim> TOP_IID = InstanceIdentifier.create(Daexim.class);
    private static final InstanceIdentifier<DaeximStatus> GLOBAL_STATUS_II = TOP_IID.child(DaeximStatus.class);
    private static final InstanceIdentifier<DaeximControl> IPC_II = TOP_IID.child(DaeximControl.class);
    private static final DataTreeIdentifier<DaeximControl> IPC_DTC = DataTreeIdentifier.of(OPERATIONAL, IPC_II);

    private final DataBroker dataBroker;
    private final DOMDataBroker domDataBroker;
    private final DOMSchemaService schemaService;
    private final NodeNameProvider nodeNameProvider;
    private final BundleContext bundleContext;
    private final AtomicBoolean skipIpcDCN = new AtomicBoolean(false);

    private final ListeningScheduledExecutorService scheduledExecutorService;
    private final InstanceIdentifier<NodeStatus> nodeStatusII;
    private final Registration rpcReg;

    private ListenableFuture<Void> exportSchedule;
    private volatile Status exportStatus = Status.Initial;
    private volatile Status importStatus = Status.Initial;
    private volatile String exportFailure = null;
    private volatile String importFailure = null;
    private volatile long lastImportTimestamp = -1;
    private volatile long lastImportChanged = -1;
    private volatile long lastExportChanged = -1;

    @Inject
    @Activate
    public DataExportImportAppProvider(@Reference DataBroker dataBroker, @Reference DOMDataBroker domDataBroker,
            @Reference DOMSchemaService schemaService, @Reference NodeNameProvider nodeNameProvider,
            @Reference RpcProviderService rpcProvider, @Reference SystemReadyMonitor systemReadyService,
            BundleContext bundleContext) {
        this.dataBroker = dataBroker;
        this.domDataBroker = domDataBroker;
        this.schemaService = schemaService;
        this.nodeNameProvider = nodeNameProvider;
        this.bundleContext = bundleContext;

        nodeStatusII = GLOBAL_STATUS_II.child(NodeStatus.class, new NodeStatusKey(nodeNameProvider.getNodeName()));
        if (readDaeximControl() != null) {
            skipIpcDCN.set(true);
        }
        dataBroker.registerTreeChangeListener(IPC_DTC, this::ipcHandler);
        updateNodeStatus();
        scheduledExecutorService = MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(10,
                ThreadFactoryProvider.builder().namePrefix("daexim-scheduler").logger(LOG).build().get()));
        LOG.info("Daexim Session Initiated, running on node '{}'", nodeNameProvider.getNodeName());

        final File bootImportConfigurationDataFile = Util.getDaeximFilePath(true, CONFIGURATION).toFile();
        final File bootImportOperationalDataFile = Util.getDaeximFilePath(true, OPERATIONAL).toFile();
        LOG.info("Checking for presence of boot import data files ({}, {})",
                bootImportConfigurationDataFile, bootImportOperationalDataFile);
        if (bootImportOperationalDataFile.exists() || bootImportConfigurationDataFile.exists()) {
            LOG.info("Daexim found files to import on boot, and will import them once the system is fully ready...");
            updateImportStatus(Status.BootImportScheduled);
            systemReadyService.registerListener(() -> {
                updateImportStatus(Status.BootImportInProgress);
                LOG.info("Daexim found files to import on boot; importing them now that the system is fully ready...");

                // FIXME: this is wrong, as highlighted by testImportOnBootWithBrokenJSON() failing sometimes:
                //        - immediateImport() fails and sets the status
                //        - then the test sees import as completed, checks the file
                //        - then this callback runs, renaming the file
                Futures.addCallback(immediateImport(new ImmediateImportInputBuilder()
                        .setCheckModels(Util.isModelFilePresent(true))
                        .setClearStores(DataStoreScope.None)
                        .setStrictDataConsistency(true)
                    .build(),
                    true), new FutureCallback<>() {
                        @Override
                        public void onSuccess(RpcResult<ImmediateImportOutput> result) {
                            if (!result.isSuccessful()
                                    || !result.getErrors().isEmpty()
                                    || !result.getResult().getResult()) {
                                failed(null);
                            } else {
                                renameBootImportFiles();
                                registerDataImportBootReady();
                            }
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            failed(throwable);
                        }

                        private void failed(Throwable throwable) {
                            renameBootImportFiles();
                            if (throwable != null) {
                                LOG.warn("Daexim import on boot failed :(", throwable);
                            } else {
                                LOG.warn("Daexim import on boot failed :(");
                            }
                        }
                    },
                    MoreExecutors.directExecutor());
            });
        } else {
            registerDataImportBootReady();
        }

        rpcReg = rpcProvider.registerRpcImplementations(ImmutableClassToInstanceMap.<Rpc<?, ?>>builder()
            .put(ScheduleExport.class, this::scheduleExport)
            .put(StatusExport.class, this::statusExport)
            .put(CancelExport.class, this::cancelExport)
            .put(ImmediateImport.class, this::immediateImport)
            .put(StatusImport.class, this::statusImport)
            .build());
    }

    void registerDataImportBootReady() {
        // publish an instance of DataImportBootReady into the OSGi service registry
        // FIXME: use org.opendaylight.infrautils.ready.order.FunctionalityReadyNotifier
        bundleContext.registerService(DataImportBootReady.class, new DataImportBootReady() { }, null);
        LOG.info("Published OSGi service {}", DataImportBootReady.class);
    }

    private static void renameBootImportFiles() {
        boolean renamedAtLeastOneFile = false;
        renamedAtLeastOneFile = renameFile(Util.getModelsFilePath(true));
        renamedAtLeastOneFile |= renameFile(Util.getDaeximFilePath(true, CONFIGURATION));
        renamedAtLeastOneFile |= renameFile(Util.getDaeximFilePath(true, OPERATIONAL));
        if (renamedAtLeastOneFile) {
            // LOG level warn instead of info just so that this message is logged in production where info may disabled
            LOG.warn("Daexim import on boot succesfully completed; renamed files to prevent re-import on next boot");
        }
    }

    private static boolean renameFile(Path file) {
        try {
            if (file.toFile().exists()) {
                final Path renamedFile = file.resolveSibling(file.getFileName().toString() + ".imported");
                Files.move(file, renamedFile, StandardCopyOption.ATOMIC_MOVE);
                // There was failure on CI because original file still exists despite using ATOMIC_MOVE
                int counter = 10;
                while (counter-- > 0) {
                    if (!file.toFile().exists()) {
                        break;
                    } else {
                        TimeUnit.MILLISECONDS.sleep(200);
                    }
                }
                if (file.toFile().exists()) {
                    throw new IllegalStateException();
                }
                LOG.info("Renamed {} to {}", file, renamedFile);
            }
            return true;
        } catch (IOException e) {
            LOG.error("Failed to rename file: {}", file.toString(), e);
            return false;
        } catch (IllegalStateException | InterruptedException e) {
            LOG.error("Failed to wait for original file to vanish: {}", file, e);
            return false;
        }
    }

    /*
     * Invoked when IPC has been posted to control data structure
     */
    private void ipcHandler(final Collection<DataTreeModification<DaeximControl>> changes) {
        if (skipIpcDCN.compareAndSet(true, false)) {
            return;
        }
        final DaeximControl newTask = changes.iterator().next().getRootNode().dataAfter();
        if (newTask != null) {
            LOG.info("IPC received : {}", newTask);
            if (newTask.getRunOnNode() != null
                    && !Objects.equals(newTask.getRunOnNode(), nodeNameProvider.getNodeName())) {
                exportFailure = null;
                updateExportStatus(Status.Skipped);
                updateNodeStatus();
                LOG.info("Export task skipped");
                return;
            }
            switch (newTask.getTaskType()) {
                case Cancel:
                    processCancel();
                    break;
                case Schedule:
                    processSchedule(newTask);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid IPC : " + newTask.getTaskType());
            }
        }
    }

    private void processSchedule(DaeximControl newTask) {
        updateExportStatus(Status.Scheduled);
        long scheduleAtTimestamp = Util.parseDate(newTask.getRunAt().getValue()).getTime();
        exportSchedule = scheduledExecutorService.schedule(new ExportTask(newTask.getIncludedModules(),
            newTask.getExcludedModules(), newTask.getStrictDataConsistency(), newTask.getSplitByModule(),
            domDataBroker, schemaService, notUsed -> updateExportStatus(Status.InProgress)),
            scheduleAtTimestamp - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        Futures.addCallback(exportSchedule, new FutureCallback<>() {
            @Override
            public void onSuccess(Void result) {
                exportFailure = null;
                updateExportStatus(Status.Complete);
                LOG.info("Export task success");
            }

            @Override
            public void onFailure(Throwable throwable) {
                if (throwable instanceof CancellationException) {
                    LOG.info("Previous export has been cancelled");
                } else {
                    LOG.error("Export failed", throwable);
                    exportFailure = throwable.getMessage();
                    updateExportStatus(Status.Failed);
                }
            }
        }, MoreExecutors.directExecutor());
    }

    private void processCancel() {
        Status newStatus = exportStatus;
        if (Status.InProgress.equals(newStatus) || Status.Scheduled.equals(newStatus)) {
            newStatus = Status.Initial;
        }
        // Cancel/Unschedule
        cancelScheduleInternal();
        updateExportStatus(newStatus);
    }

    /*
     * Invoke IPC
     */
    private void invokeIPC(DaeximControl ctl) throws InterruptedException, ExecutionException {
        final WriteTransaction wTrx = dataBroker.newWriteOnlyTransaction();
        wTrx.put(OPERATIONAL, IPC_II, ctl);
        wTrx.commit().get();
    }

    /*
     * Update status of local node
     */
    private synchronized void updateNodeStatus() {
        final WriteTransaction wTrx = dataBroker.newWriteOnlyTransaction();
        wTrx.put(LogicalDatastoreType.OPERATIONAL, nodeStatusII, createNodeStatusData());
        try {
            wTrx.commit().get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to update local node status", e);
        }
    }

    private NodeStatus createNodeStatusData() {
        final NodeStatusBuilder nsb = new NodeStatusBuilder().setExportStatus(exportStatus)
                .setExportResult(exportFailure).setImportStatus(importStatus).setImportResult(importFailure)
                .setDataFiles(Util.collectDataFiles(false).values().stream()
                    .map(File::getAbsolutePath)
                    .collect(ImmutableSet.toImmutableSet()))
                .setNodeName(nodeNameProvider.getNodeName())
                .setModelFile(Util.isModelFilePresent(false) ? Util.getModelsFilePath(false).toString() : null);
        nsb.setLastExportChange(
                lastExportChanged != -1 ? new AbsoluteTime(Util.toDateAndTime(new Date(lastExportChanged))) : null);
        nsb.setLastImportChange(
                lastImportChanged != -1 ? new AbsoluteTime(Util.toDateAndTime(new Date(lastImportChanged))) : null);
        nsb.setImportedAt(
                lastImportTimestamp != -1 ? new AbsoluteTime(Util.toDateAndTime(new Date(lastImportTimestamp))) : null);
        return nsb.build();
    }

    @Nullable
    private DaeximControl readDaeximControl() {
        final ListenableFuture<Optional<DaeximControl>> future;
        try (ReadTransaction roTrx = dataBroker.newReadOnlyTransaction()) {
            future = roTrx.read(LogicalDatastoreType.OPERATIONAL, IPC_II);
        }

        try {
            return future.get().orElse(null);
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn("Failed to read IPC", e);
            return null;
        }
    }

    /*
     * Read global status
     */
    private DaeximStatus readGlobalStatus() throws InterruptedException, ExecutionException {
        final ListenableFuture<Optional<DaeximStatus>> future;
        try (ReadTransaction roTrx = dataBroker.newReadOnlyTransaction()) {
            future = roTrx.read(LogicalDatastoreType.OPERATIONAL, GLOBAL_STATUS_II);
        }

        // After restore, our top level elements are gone
        final Optional<DaeximStatus> opt = future.get();
        if (opt.isPresent()) {
            return opt.get();
        } else {
            return rebuildGlobalStatus();
        }
    }

    /*
     * Initializes status of local node and return global status with local node's
     * status included in it
     */
    private DaeximStatus rebuildGlobalStatus() {
        exportStatus = Status.Initial;
        importStatus = Status.Initial;
        LOG.info("Global status is not yet created");
        updateNodeStatus();
        return new DaeximStatusBuilder().setNodeStatus(BindingMap.of(createNodeStatusData())).build();
    }

    /**
     * This function calculate runtime global status.<br/>
     * Here are rules:
     * <p/>
     * <ol>
     * <li>If all nodes are {@link Status#Complete} then then return
     * {@link Status#Complete}</li>
     * <li>If at least one node is {@link Status#InProgress} then return
     * {@link Status#InProgress}</li>
     * <li>If at least one node is {@link Status#Scheduled} then return
     * {@link Status#Scheduled}</li>
     * <li>If at least one node is {@link Status#Failed} then return
     * {@link Status#Failed}</li>
     * <li>If all nodes are {@link Status#Initial} then then return
     * {@link Status#Initial}</li>
     * <li>If none of previous are true, then return
     * {@value Status#Inconsistent}
     * </ol>
     */
    @VisibleForTesting
    Status calculateStatus(final List<Nodes> nodes) {
        boolean inProgress = false;
        boolean isComplete = true;
        boolean isFailed = false;
        boolean isInitial = true;
        boolean isScheduled = false;
        for (final Nodes t : nodes) {
            switch (t.getStatus()) {
            // at least one export is in progress
                case InProgress:
                    isInitial = false;
                    isComplete = false;
                    inProgress = true;
                    break;
                // all nodes completed their job, we are done
                case Complete:
                    isComplete &= true;
                    break;
                // any node failed
                case Failed:
                    isInitial = false;
                    isComplete = false;
                    isFailed = true;
                    break;
                // all nodes are in initial status
                case Initial:
                    isComplete = false;
                    isInitial &= true;
                    break;
                case Scheduled:
                    isInitial = false;
                    isComplete = false;
                    isScheduled = true;
                    break;
                default:
                    break;
            }
        }
        if (isComplete) {
            return Status.Complete;
        }
        if (inProgress) {
            return Status.InProgress;
        }
        if (isScheduled) {
            return Status.Scheduled;
        }
        if (isFailed) {
            return Status.Failed;
        }
        if (isInitial) {
            return Status.Initial;
        }
        return Status.Inconsistent;
    }

    private void cancelScheduleInternal() {
        if (exportSchedule != null) {
            exportSchedule.cancel(true);
            exportSchedule = null;
        }
        updateExportStatus(Status.Initial);
    }

    /**
     * Method called when provider is about to close.
     */
    @Override
    @PreDestroy
    @Deactivate
    public void close() {
        rpcReg.close();
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        LOG.info("{} closed", getClass().getSimpleName());
    }

    // RPC Methods

    /**
     * Cancels any pending or active export tasks.
     */
    @VisibleForTesting
    ListenableFuture<RpcResult<CancelExportOutput>> cancelExport(CancelExportInput input) {
        try {
            // FIXME: this should be asynchronous!
            invokeIPC(new DaeximControlBuilder().setTaskType(IpcType.Cancel).build());
            return RpcResultBuilder.<CancelExportOutput>success(new CancelExportOutputBuilder()
                .setResult(true)
                .build()).buildFuture();
        } catch (ExecutionException | InterruptedException e) {
            return RpcResultBuilder.<CancelExportOutput>failed()
                .withResult(new CancelExportOutputBuilder()
                    .setResult(false)
                    .setReason(e.getMessage())
                    .build())
                .withError(ErrorType.APPLICATION, e.getMessage(), e)
                .buildFuture();
        }
    }

    /**
     * Schedule export.
     */
    @VisibleForTesting
    ListenableFuture<RpcResult<ScheduleExportOutput>> scheduleExport(ScheduleExportInput input) {
        Objects.requireNonNull(input, "input");
        awaitBootImport("DataExportImport.scheduleExport()");
        long scheduleAtTimestamp;
        final ScheduleExportOutputBuilder outputBuilder = new ScheduleExportOutputBuilder();
        final RunAt runAt = input.getRunAt();
        if (runAt == null) {
            return RpcResultBuilder.<ScheduleExportOutput>failed()
                .withError(ErrorType.PROTOCOL, "No schedule info present in request (run-at)")
                .buildFuture();
        }
        final String scheduledString;
        if (runAt.getRelativeTime() != null) {
            scheduleAtTimestamp = System.currentTimeMillis() + runAt.getRelativeTime().getValue().toJava() * 10;
            scheduledString = Util.dateToUtcString(new Date(scheduleAtTimestamp));
        } else {
            scheduledString = runAt.getAbsoluteTime().getValue();
            scheduleAtTimestamp = Util.parseDate(scheduledString).getTime();
        }
        // verify that we are not trying to schedule in the past
        if (scheduleAtTimestamp < System.currentTimeMillis()) {
            return RpcResultBuilder.<ScheduleExportOutput>failed()
                .withError(ErrorType.PROTOCOL, "Attempt to schedule export in past")
                .buildFuture();
        }
        cancelScheduleInternal();
        LOG.info("Scheduling export at {}, which is {} seconds in the future", scheduledString,
            (scheduleAtTimestamp - System.currentTimeMillis()) / 1000);
        try {
            final DaeximControlBuilder builder = new DaeximControlBuilder()
                .setTaskType(IpcType.Schedule)
                .setStrictDataConsistency(input.getStrictDataConsistency())
                .setIncludedModules(input.getIncludedModules())
                .setExcludedModules(input.getExcludedModules())
                .setRunAt(new AbsoluteTime(new DateAndTime(Util.toDateAndTime(new Date(scheduleAtTimestamp)))))
                .setSplitByModule(input.getSplitByModule());
            if (Boolean.TRUE.equals(input.getLocalNodeOnly())) {
                builder.setRunOnNode(nodeNameProvider.getNodeName());
            }
            // FIXME: this should be asynchronous!
            invokeIPC(builder.build());
            outputBuilder.setResult(true);
            return RpcResultBuilder.<ScheduleExportOutput>success(outputBuilder.build()).buildFuture();
        } catch (ExecutionException | InterruptedException e) {
            LOG.error("scheduleExport() failed", e);
            outputBuilder.setResult(false);
            return RpcResultBuilder.<ScheduleExportOutput>failed()
                .withError(ErrorType.APPLICATION, e.getMessage(), e).withResult(outputBuilder.build())
                .buildFuture();

        }
    }

    /**
     * Pending export status.
     */
    ListenableFuture<RpcResult<StatusExportOutput>> statusExport(StatusExportInput input) {
        final StatusExportOutputBuilder builder = new StatusExportOutputBuilder();
        try {
            final DaeximStatus gs = readGlobalStatus();
            final List<Nodes> tasks = gs.nonnullNodeStatus().values().stream()
                .map(nodeStatus -> {
                    final var nb = new NodesBuilder()
                        .setReason(nodeStatus.getExportResult())
                        .withKey(new NodesKey(nodeStatus.getNodeName()))
                        .setStatus(nodeStatus.getExportStatus());
                    if (Status.Complete.equals(nodeStatus.getExportStatus())) {
                        nb.setModelFile(nodeStatus.getModelFile()).setDataFiles(nodeStatus.getDataFiles());
                    }
                    return nb
                        .setLastChange(nodeStatus.getLastExportChange())
                        .build();
                })
                .collect(Collectors.toList());
            final Status s = calculateStatus(tasks);
            builder.setStatus(s);
            if (Status.Scheduled.equals(s)) {
                final DaeximControl ctrl = readDaeximControl();
                if (ctrl == null) {
                    return RpcResultBuilder.<StatusExportOutput>failed()
                        .withError(ErrorType.APPLICATION, "Missing control data")
                        .buildFuture();
                }
                builder.setRunAt(ctrl.getRunAt());
            }
            builder.setNodes(BindingMap.of(tasks));
            return RpcResultBuilder.<StatusExportOutput>success(builder.build()).buildFuture();
        } catch (ExecutionException | InterruptedException e) {
            LOG.error("statusExport() failed", e);
            return RpcResultBuilder.<StatusExportOutput>failed()
                .withError(ErrorType.APPLICATION, e.getMessage(), e)
                .buildFuture();
        }
    }

    /**
     * Immediate restore operation.
     */
    @VisibleForTesting
    ListenableFuture<RpcResult<ImmediateImportOutput>> immediateImport(ImmediateImportInput input) {
        Objects.requireNonNull(input, "input");
        awaitBootImport("DataExportImport.immediateImport()");
        return immediateImport(input, false);
    }

    private ListenableFuture<RpcResult<ImmediateImportOutput>> immediateImport(ImmediateImportInput input,
            boolean isBooting) {
        final var importFuture = scheduledExecutorService.submit(
            new ImportTask(input, domDataBroker, schemaService, isBooting, () -> {
                if (!isBooting) {
                    // if isBooting then we've set another status before calling this
                    // (it's important that happens immediately, without waiting for the Executor)
                    updateImportStatus(Status.InProgress);
                }
            }));
        Futures.addCallback(importFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(ImportOperationResult result) {
                LOG.info("Restore operation finished : {}", result);
                if (!result.getResult()) {
                    importFailure = result.getReason();
                    if (isBooting) {
                        updateImportStatus(Status.BootImportFailed);
                    } else {
                        updateImportStatus(Status.Failed);
                    }
                } else {
                    lastImportTimestamp = System.currentTimeMillis();
                    updateImportStatus(Status.Complete);
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                LOG.info("Restore operation failed", throwable);
                lastImportTimestamp = -1;
                importFailure = throwable.getMessage();
                updateImportStatus(Status.Failed);
            }
        }, MoreExecutors.directExecutor());
        return Futures.transform(importFuture, inputResult -> {
            final ImmediateImportOutputBuilder output = new ImmediateImportOutputBuilder();
            output.setResult(inputResult.getResult());
            if (!inputResult.getResult()) {
                output.setReason(inputResult.getReason());
                return RpcResultBuilder.<ImmediateImportOutput>success().withResult(output.build()).build();
            }
            return RpcResultBuilder.<ImmediateImportOutput>success(output.build()).build();
        }, MoreExecutors.directExecutor());
    }

    @Override
    public OperationStatus statusImportOnLocalNode() {
        return new StatusImportOutputBuilder().setStatus(importStatus).setReason(importFailure).build();
    }

    @Override
    @Deprecated
    public void awaitBootImport(String blockingWhat) {
        if (Status.BootImportFailed.equals(importStatus)) {
            throw new IllegalStateException(importFailure);
        } else if (Status.BootImportInProgress.equals(importStatus)) {
            while (Status.BootImportInProgress.equals(importStatus)) {
                LOG.warn("awaitBootImport() blocking {}, waiting 5s more...", blockingWhat);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    LOG.error("awaitBootImport() boot-import-in-progress InterruptedException "
                            + "- import not finished, returning anyway", e);
                    return;
                }
            }
            // recursive self invocation just to avoid copy/paste of BootImportFailed handling
            awaitBootImport(blockingWhat);
        }
    }

    /**
     * Import status RPC.
     */
    @VisibleForTesting
    ListenableFuture<RpcResult<StatusImportOutput>> statusImport(StatusImportInput input) {
        final StatusImportOutputBuilder builder = new StatusImportOutputBuilder();
        try {
            final DaeximStatus gs = readGlobalStatus();
            final var nodes = gs.nonnullNodeStatus().values().stream().map(nodeStatus -> {
                final org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status._import.output
                       .NodesBuilder nb = new org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921
                               .status._import.output.NodesBuilder();
                if (Status.Complete.equals(nodeStatus.getImportStatus())) {
                    nb.setImportedAt(nodeStatus.getImportedAt());
                }
                nb.setReason(nodeStatus.getImportResult());
                nb.setModelFile(nodeStatus.getModelFile());
                nb.setDataFiles(nodeStatus.getDataFiles());
                nb.setStatus(nodeStatus.getImportStatus());
                if (nodeStatus.getLastImportChange() != null) {
                    nb.setLastChange(nodeStatus.getLastImportChange());
                }
                nb.withKey(new org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921
                        .status._import.output.NodesKey(nodeStatus.getNodeName()));
                return nb.build();
            }).collect(BindingMap.toMap());
            builder.setStatus(importStatus);
            builder.setNodes(nodes);
            return RpcResultBuilder.<StatusImportOutput>success(builder.build()).buildFuture();
        } catch (ExecutionException | InterruptedException e) {
            LOG.error("statusImport() failed", e);
            return RpcResultBuilder.<StatusImportOutput>failed()
                .withError(ErrorType.APPLICATION, e.getMessage(), e)
                .buildFuture();
        }
    }

    private void updateExportStatus(Status newStatus) {
        if (!exportStatus.equals(newStatus)) {
            lastExportChanged = System.currentTimeMillis();
            LOG.debug("Export status transition from {} to {} at {}", exportStatus, newStatus, lastExportChanged);
            exportStatus = newStatus;
            updateNodeStatus();
        }
    }

    private void updateImportStatus(Status newStatus) {
        if (!importStatus.equals(newStatus)) {
            lastImportChanged = System.currentTimeMillis();
            LOG.debug("Import status transition from {} to {} at {}", importStatus, newStatus, lastImportChanged);
            importStatus = newStatus;
            updateNodeStatus();
        }
    }
}
