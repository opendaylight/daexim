/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import java.io.File;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.opendaylight.controller.md.sal.binding.api.ClusteredDataTreeChangeListener;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.DataTreeIdentifier;
import org.opendaylight.controller.md.sal.binding.api.DataTreeModification;
import org.opendaylight.controller.md.sal.binding.api.ReadOnlyTransaction;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.ReadFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.controller.md.sal.dom.api.DOMDataBroker;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.RpcRegistration;
import org.opendaylight.controller.sal.binding.api.RpcProviderRegistry;
import org.opendaylight.controller.sal.core.api.model.SchemaService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.Daexim;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.DaeximBuilder;
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
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.CancelExportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.CancelExportOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataExportImportService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportInput.RunAt;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ScheduleExportOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.Status;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusExportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusExportOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusImportOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.StatusImportOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status.export.output.Nodes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status.export.output.NodesBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status.export.output.NodesKey;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev130715.DateAndTime;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcError.ErrorType;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
/* Logging */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.opendaylight.daexim.spi.NodeNameProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class DataExportImportAppProvider implements DataExportImportService, AutoCloseable {
    private static final String LOG_MSG_SCHEDULING_EXPORT = "Scheduling export at %s, which is %d seconds in future";
    private static final Logger LOG = LoggerFactory.getLogger(DataExportImportAppProvider.class);
    private DOMDataBroker domDataBroker;
    private SchemaService schemaService;
    private RpcProviderRegistry rpcProviderRegistry;
    private NodeNameProvider nodeNameProvider;
    private RpcRegistration<DataExportImportService> reg;
    private ListenableFuture<Void> exportSchedule;
    private ListeningScheduledExecutorService scheduledExecutorService;
    private volatile Status exportStatus = Status.Initial;
    private volatile Status importStatus = Status.Initial;
    private volatile String exportFailure = null;
    private volatile String importFailure = null;
    private volatile long lastImportTimestamp = -1;
    private volatile long lastImportChanged = -1;
    private volatile long lastExportChanged = -1;
    private DataBroker dataBroker;
    private InstanceIdentifier<DaeximControl> ipcII;
    private InstanceIdentifier<NodeStatus> nodeStatusII;
    private InstanceIdentifier<DaeximStatus> globalStatusII;
    private DataTreeIdentifier<DaeximControl> ipcDTC;
    private InstanceIdentifier<Daexim> topII;

    /**
     * Method called when the blueprint container is created.
     */
    public void init() {
        topII = InstanceIdentifier.create(Daexim.class);
        globalStatusII = InstanceIdentifier.create(Daexim.class).child(DaeximStatus.class);
        ipcII = InstanceIdentifier.create(Daexim.class).child(DaeximControl.class);
        nodeStatusII = InstanceIdentifier.create(Daexim.class).child(DaeximStatus.class).child(NodeStatus.class,
                new NodeStatusKey(nodeNameProvider.getNodeName()));
        ipcDTC = new DataTreeIdentifier<>(LogicalDatastoreType.OPERATIONAL, ipcII);
        dataBroker.registerDataTreeChangeListener(ipcDTC, new ClusteredDataTreeChangeListener<DaeximControl>() {
            @Override
            public void onDataTreeChanged(Collection<DataTreeModification<DaeximControl>> changes) {
                try {
                    ipcHandler(changes);
                } catch (TransactionCommitFailedException e) {
                    LOG.error("Failure while processing IPC request", e);
                }
            }
        });
        checkDatastructures();
        scheduledExecutorService = MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(10,
                new ThreadFactoryBuilder().setNameFormat("daexim-scheduler-%d").build()));
        reg = rpcProviderRegistry.addRpcImplementation(DataExportImportService.class, this);
        LOG.info("Daexim Session Initiated, running on node '{}'", nodeNameProvider.getNodeName());
    }

    private void checkDatastructures() {
        final ReadOnlyTransaction roTrx = dataBroker.newReadOnlyTransaction();
        try {
            if (roTrx.read(LogicalDatastoreType.OPERATIONAL, globalStatusII).checkedGet().orNull() == null) {
                rebuildGlobalStatus();
            } else {
                updateNodeStatus();
            }
        } catch (ReadFailedException | TransactionCommitFailedException e) {
            LOG.error("Failed to check global status structure", e);
        } finally {
            roTrx.close();
        }
    }

    private DaeximStatus rebuildGlobalStatus() throws TransactionCommitFailedException {
        exportStatus = Status.Initial;
        importStatus = Status.Initial;
        LOG.info("Global status is not yet created");
        final DaeximStatus globalStatus = new DaeximStatusBuilder().setExportStatus(exportStatus)
                .setImportStatus(importStatus).setNodeStatus(Lists.<NodeStatus>newArrayList(createNodeStatusData()))
                .build();
        final WriteTransaction wTrx = dataBroker.newWriteOnlyTransaction();
        wTrx.put(LogicalDatastoreType.OPERATIONAL, topII, new DaeximBuilder().setDaeximStatus(globalStatus).build());
        wTrx.submit().checkedGet();
        return globalStatus;
    }

    /*
     * Invoked when IPC has been posted to control data structure
     */
    private void ipcHandler(final Collection<DataTreeModification<DaeximControl>> changes)
            throws TransactionCommitFailedException {
        final DaeximControl newTask = changes.iterator().next().getRootNode().getDataAfter();
        if (newTask != null) {
            LOG.info("IPC received : {}", newTask);
            if (IpcType.Schedule.equals(newTask.getTaskType())) {
                // Schedule
                updateExportStatus(Status.Scheduled);
                updateNodeStatus();
                long scheduleAtTimestamp = Util.parseDate(newTask.getRunAt().getValue()).getTime();
                exportSchedule = scheduledExecutorService.schedule(
                        new ExportTask(newTask.getExcludedModules(), domDataBroker, schemaService, new Callback() {
                            @Override
                            public void call() throws Exception {
                                updateExportStatus(Status.InProgress);
                                updateNodeStatus();
                            }
                        }), scheduleAtTimestamp - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                Futures.addCallback(exportSchedule, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(Void result) {
                        exportFailure = null;
                        updateExportStatus(Status.Complete);
                        updateNodeStatus();
                        LOG.info("Export task success");
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        if (t instanceof CancellationException) {
                            LOG.info("Previous export has been cancelled");
                        } else {
                            LOG.error("Export failed", t);
                            exportFailure = t.getMessage();
                            updateExportStatus(Status.Failed);
                            updateNodeStatus();
                        }
                    }
                });
                return;
            }
            if (IpcType.Cancel.equals(newTask.getTaskType())) {
                Status newStatus = exportStatus;
                if (Status.InProgress.equals(newStatus) || Status.Scheduled.equals(newStatus)) {
                    newStatus = Status.Initial;
                }
                // Cancel/Unschedule
                cancelScheduleInternal();
                updateExportStatus(newStatus);
                updateNodeStatus();
                return;
            }
        }
    }

    /*
     * Invoke IPC
     */
    private void invokeIPC(DaeximControl ctl) throws TransactionCommitFailedException {
        final WriteTransaction wTrx = dataBroker.newWriteOnlyTransaction();
        wTrx.put(LogicalDatastoreType.OPERATIONAL, ipcII, ctl);
        wTrx.submit().checkedGet();
    }

    /*
     * Update status of local node
     */
    private void updateNodeStatus() {
        final WriteTransaction wTrx = dataBroker.newWriteOnlyTransaction();
        wTrx.put(LogicalDatastoreType.OPERATIONAL, nodeStatusII, createNodeStatusData());
        try {
            wTrx.submit().checkedGet();
        } catch (TransactionCommitFailedException e) {
            LOG.error("Failed to update local node status", e);
        }
    }

    private NodeStatus createNodeStatusData() {
        final NodeStatusBuilder nsb = new NodeStatusBuilder().setExportStatus(exportStatus)
                .setExportResult(exportFailure).setImportStatus(importStatus).setImportResult(importFailure)
                .setDataFiles(Lists.transform(Lists.newArrayList(Util.collectDataFiles().values()),
                        new Function<File, String>() {
                            @Override
                            public String apply(File input) {
                                return input.getAbsolutePath();
                            }
                        }))
                .setNodeName(nodeNameProvider.getNodeName())
                .setModelFile(Util.isModelFilePresent() ? Util.getModelsFilePath().toString() : null);
        nsb.setLastExportChange(
                lastExportChanged != -1 ? new AbsoluteTime(Util.toDateAndTime(new Date(lastExportChanged))) : null);
        nsb.setLastImportChange(
                lastImportChanged != -1 ? new AbsoluteTime(Util.toDateAndTime(new Date(lastImportChanged))) : null);
        nsb.setImportedAt(
                lastImportTimestamp != -1 ? new AbsoluteTime(Util.toDateAndTime(new Date(lastImportTimestamp))) : null);
        return nsb.build();
    }

    private DaeximControl readDaeximControl() throws ReadFailedException {
        final ReadOnlyTransaction roTrx = dataBroker.newReadOnlyTransaction();
        try {
            return roTrx.read(LogicalDatastoreType.OPERATIONAL, ipcII).checkedGet().orNull();
        } finally {
            roTrx.close();
        }
    }

    /*
     * Read global status
     */
    private DaeximStatus readGlobalStatus() throws ReadFailedException, TransactionCommitFailedException {
        final ReadOnlyTransaction roTrx = dataBroker.newReadOnlyTransaction();
        try {
            // After restore, our top level elements are gone
            final Optional<DaeximStatus> opt = roTrx.read(LogicalDatastoreType.OPERATIONAL, globalStatusII)
                    .checkedGet();
            if (opt.isPresent()) {
                return opt.get();
            } else {
                return rebuildGlobalStatus();
            }
        } finally {
            roTrx.close();
        }
    }

    /**
     * This function calculate runtime global status.</br>
     * Here are rules:
     * </p>
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
     * 
     * @param tasks
     * @return
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
    public void close() {
        reg.close();
        scheduledExecutorService.shutdownNow();
        LOG.info("{} closed", getClass().getSimpleName());
    }

    /**
     * RPC Methods
     */

    /**
     * Cancels any pending or active export tasks
     *
     */
    @Override
    public Future<RpcResult<CancelExportOutput>> cancelExport() {
        final CancelExportOutputBuilder outputBuilder = new CancelExportOutputBuilder();
        try {
            invokeIPC(new DaeximControlBuilder().setTaskType(IpcType.Cancel).build());
            outputBuilder.setResult(true);
            return Futures.immediateFuture(RpcResultBuilder.<CancelExportOutput>success(outputBuilder.build()).build());
        } catch (TransactionCommitFailedException e) {
            outputBuilder.setResult(false);
            outputBuilder.setReason(e.getMessage());
            return Futures.immediateFuture(
                    RpcResultBuilder.<CancelExportOutput>failed().withResult(outputBuilder.build()).build());
        }
    }

    /**
     * Schedule export
     *
     */
    @Override
    public Future<RpcResult<ScheduleExportOutput>> scheduleExport(ScheduleExportInput input) {
        long scheduleAtTimestamp;
        final ScheduleExportOutputBuilder outputBuilder = new ScheduleExportOutputBuilder();
        final RunAt runAt = input.getRunAt();
        if (runAt == null) {
            return Futures.immediateFuture(RpcResultBuilder.<ScheduleExportOutput>failed()
                    .withError(ErrorType.PROTOCOL, "No schedule info present in request (run-at)").build());
        }
        final String logMsg;
        if (runAt.getRelativeTime() != null) {
            scheduleAtTimestamp = System.currentTimeMillis() + (runAt.getRelativeTime().getValue() * 10);
            logMsg = String.format(LOG_MSG_SCHEDULING_EXPORT, Util.dateToUtcString(new Date(scheduleAtTimestamp)),
                    (scheduleAtTimestamp - System.currentTimeMillis()) / 1000);
        } else {
            scheduleAtTimestamp = Util.parseDate(runAt.getAbsoluteTime().getValue()).getTime();
            logMsg = String.format(LOG_MSG_SCHEDULING_EXPORT, runAt.getAbsoluteTime().getValue(),
                    (scheduleAtTimestamp - System.currentTimeMillis()) / 1000);
        }
        // verify that we are not trying to schedule in past
        if (scheduleAtTimestamp < System.currentTimeMillis()) {
            return Futures.immediateFuture(RpcResultBuilder.<ScheduleExportOutput>failed()
                    .withError(ErrorType.PROTOCOL, "Attempt to schedule export in past").build());
        }
        cancelScheduleInternal();
        LOG.info(logMsg);
        try {
            final DaeximControlBuilder builder = new DaeximControlBuilder();
            builder.setTaskType(IpcType.Schedule);
            builder.setExcludedModules(input.getExcludedModules());
            builder.setRunAt(new AbsoluteTime(new DateAndTime(Util.toDateAndTime(new Date(scheduleAtTimestamp)))));
            invokeIPC(builder.build());
            outputBuilder.setResult(true);
            return Futures
                    .immediateFuture(RpcResultBuilder.<ScheduleExportOutput>success(outputBuilder.build()).build());
        } catch (TransactionCommitFailedException e) {
            outputBuilder.setResult(false);
            return Futures.immediateFuture(RpcResultBuilder.<ScheduleExportOutput>failed()
                    .withError(ErrorType.APPLICATION, e.getMessage()).withResult(outputBuilder.build()).build());

        }
    }

    /**
     * Pending export status
     *
     */
    @Override
    public Future<RpcResult<StatusExportOutput>> statusExport() {
        final StatusExportOutputBuilder builder = new StatusExportOutputBuilder();
        try {
            final DaeximStatus gs = readGlobalStatus();
            final List<Nodes> tasks = Lists
                    .<Nodes>newArrayList(Iterables.transform(gs.getNodeStatus(), new Function<NodeStatus, Nodes>() {
                        @Override
                        public Nodes apply(NodeStatus input) {
                            final NodesBuilder nb = new NodesBuilder().setReason(input.getExportResult())
                                    .setKey(new NodesKey(input.getNodeName())).setStatus(input.getExportStatus());
                            if (Status.Complete.equals(input.getExportStatus())) {
                                nb.setModelFile(input.getModelFile()).setDataFiles(input.getDataFiles());
                            }
                            nb.setLastChange(input.getLastExportChange());
                            return nb.build();
                        }
                    }));
            final Status s = calculateStatus(tasks);
            builder.setStatus(s);
            if (Status.Scheduled.equals(s)) {
                builder.setRunAt(readDaeximControl().getRunAt());
            }
            builder.setNodes(tasks);
            return Futures.immediateFuture(RpcResultBuilder.<StatusExportOutput>success(builder.build()).build());
        } catch (ReadFailedException | TransactionCommitFailedException e) {
            return Futures.immediateFuture(RpcResultBuilder.<StatusExportOutput>failed()
                    .withError(ErrorType.APPLICATION, e.getMessage()).build());
        }
    }

    /**
     * Immediate restore operation
     */
    @Override
    public Future<RpcResult<ImmediateImportOutput>> immediateImport(ImmediateImportInput input) {
        final ListenableFuture<ImportOperationResult> f = scheduledExecutorService
                .submit(new ImportTask(input, domDataBroker, schemaService, new Callback() {
                    @Override
                    public void call() throws Exception {
                        updateImportStatus(Status.InProgress);
                        updateNodeStatus();
                    }
                }));
        Futures.addCallback(f, new FutureCallback<ImportOperationResult>() {
            @Override
            public void onSuccess(ImportOperationResult result) {
                LOG.info("Restore operation finished : {}", result);
                if (!result.isResult()) {
                    updateImportStatus(Status.Failed);
                    importFailure = result.getReason();
                } else {
                    lastImportTimestamp = System.currentTimeMillis();
                    updateImportStatus(Status.Complete);
                }
                updateNodeStatus();
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.info("Restore operation failed", t);
                lastImportTimestamp = -1;
                updateImportStatus(Status.Failed);
                importFailure = t.getMessage();
                updateNodeStatus();
            }
        });
        return Futures.transform(f, new Function<ImportOperationResult, RpcResult<ImmediateImportOutput>>() {
            @Override
            public RpcResult<ImmediateImportOutput> apply(ImportOperationResult input) {
                final ImmediateImportOutputBuilder output = new ImmediateImportOutputBuilder();
                output.setResult(input.isResult());
                if (!input.isResult()) {
                    output.setReason(input.getReason());
                    return RpcResultBuilder.<ImmediateImportOutput>success().withResult(output.build()).build();
                }
                return RpcResultBuilder.<ImmediateImportOutput>success(output.build()).build();
            }
        });
    }

    /**
     * Import status RPC
     */
    @Override
    public Future<RpcResult<StatusImportOutput>> statusImport() {
        final StatusImportOutputBuilder builder = new StatusImportOutputBuilder();
        try {
            final DaeximStatus gs = readGlobalStatus();
            final List<org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status._import.output.Nodes> nodes = Lists.<org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status._import.output.Nodes>newArrayList(
                    Iterables.transform(gs.getNodeStatus(),
                            new Function<NodeStatus, org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status._import.output.Nodes>() {
                                @Override
                                public org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status._import.output.Nodes apply(
                                        NodeStatus input) {
                                    final org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status._import.output.NodesBuilder nb = new org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status._import.output.NodesBuilder();
                                    if (Status.Complete.equals(input.getImportStatus())) {
                                        nb.setImportedAt(input.getImportedAt());
                                    }
                                    nb.setReason(input.getImportResult());
                                    nb.setModelFile(input.getModelFile());
                                    nb.setDataFiles(input.getDataFiles());
                                    nb.setStatus(input.getImportStatus());
                                    if (input.getLastImportChange() != null) {
                                        nb.setLastChange(input.getLastImportChange());
                                    }
                                    nb.setKey(
                                            new org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.status._import.output.NodesKey(
                                                    input.getNodeName()));
                                    return nb.build();
                                }
                            }));
            builder.setStatus(importStatus);
            builder.setNodes(nodes);
            return Futures.immediateFuture(RpcResultBuilder.<StatusImportOutput>success(builder.build()).build());
        } catch (ReadFailedException | TransactionCommitFailedException e) {
            return Futures.immediateFuture(RpcResultBuilder.<StatusImportOutput>failed()
                    .withError(ErrorType.APPLICATION, e.getMessage()).build());
        }
    }

    private void updateExportStatus(Status newStatus) {
        if (!exportStatus.equals(newStatus)) {
            lastExportChanged = System.currentTimeMillis();
            LOG.debug("Export status transition from {} to {} at {}", exportStatus, newStatus, lastExportChanged);
            exportStatus = newStatus;
        }
    }

    private void updateImportStatus(Status newStatus) {
        if (!importStatus.equals(newStatus)) {
            lastImportChanged = System.currentTimeMillis();
            LOG.debug("Import status transition from {} to {} at {}", importStatus, newStatus, lastImportChanged);
            importStatus = newStatus;
        }
    }

    /*
     * Public setters
     */

    public void setDomDataBroker(DOMDataBroker domDataBroker) {
        this.domDataBroker = domDataBroker;
    }

    public void setSchemaService(SchemaService schemaService) {
        this.schemaService = schemaService;
    }

    public void setRpcProviderRegistry(RpcProviderRegistry rpcProviderRegistry) {
        this.rpcProviderRegistry = rpcProviderRegistry;
    }

    public void setNodeNameProvider(NodeNameProvider nodeNameProvider) {
        this.nodeNameProvider = nodeNameProvider;
    }

    public void setDataBroker(DataBroker dataBroker) {
        this.dataBroker = dataBroker;
    }
}
