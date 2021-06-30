/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.opendaylight.daexim.impl.model.internal.Model;
import org.opendaylight.daexim.impl.model.internal.ModelsNotAvailableException;
import org.opendaylight.mdsal.common.api.LogicalDatastoreType;
import org.opendaylight.mdsal.dom.api.DOMDataBroker;
import org.opendaylight.mdsal.dom.api.DOMDataTreeReadWriteTransaction;
import org.opendaylight.mdsal.dom.api.DOMDataTreeWriteTransaction;
import org.opendaylight.mdsal.dom.api.DOMSchemaService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.ImportOperationResult;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.ImportOperationResultBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataStoreScope;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInput;
import org.opendaylight.yangtools.yang.common.QName;
import org.opendaylight.yangtools.yang.common.Revision;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier.NodeIdentifier;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier.PathArgument;
import org.opendaylight.yangtools.yang.data.api.schema.DataContainerChild;
import org.opendaylight.yangtools.yang.data.api.schema.MapEntryNode;
import org.opendaylight.yangtools.yang.data.api.schema.MapNode;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNode;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNodeContainer;
import org.opendaylight.yangtools.yang.data.api.schema.UnkeyedListEntryNode;
import org.opendaylight.yangtools.yang.data.api.schema.UnkeyedListNode;
import org.opendaylight.yangtools.yang.data.api.schema.stream.NormalizedNodeStreamWriter;
import org.opendaylight.yangtools.yang.data.codec.gson.JSONCodecFactorySupplier;
import org.opendaylight.yangtools.yang.data.codec.gson.JsonParserStream;
import org.opendaylight.yangtools.yang.data.impl.schema.ImmutableNodes;
import org.opendaylight.yangtools.yang.data.impl.schema.ImmutableNormalizedNodeStreamWriter;
import org.opendaylight.yangtools.yang.data.impl.schema.builder.api.CollectionNodeBuilder;
import org.opendaylight.yangtools.yang.data.impl.schema.builder.api.NormalizedNodeContainerBuilder;
import org.opendaylight.yangtools.yang.data.impl.schema.builder.impl.ImmutableContainerNodeBuilder;
import org.opendaylight.yangtools.yang.data.impl.schema.builder.impl.ImmutableMapNodeBuilder;
import org.opendaylight.yangtools.yang.data.impl.schema.builder.impl.ImmutableOrderedMapNodeBuilder;
import org.opendaylight.yangtools.yang.data.impl.schema.builder.impl.ImmutableUnkeyedListNodeBuilder;
import org.opendaylight.yangtools.yang.data.util.DataSchemaContextNode;
import org.opendaylight.yangtools.yang.data.util.DataSchemaContextTree;
import org.opendaylight.yangtools.yang.model.api.DataSchemaNode;
import org.opendaylight.yangtools.yang.model.api.ListSchemaNode;
import org.opendaylight.yangtools.yang.model.api.Module;
import org.opendaylight.yangtools.yang.model.api.SchemaContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportTask implements Callable<ImportOperationResult> {
    private static final Logger LOG = LoggerFactory.getLogger(ImportTask.class);
    private static final JSONCodecFactorySupplier CODEC = JSONCodecFactorySupplier.DRAFT_LHOTKA_NETMOD_YANG_JSON_02;
    private static final short DEFAULT_MAX_TRAVERSAL_DEPTH = 2;
    private static final int DEFAULT_LIST_BATCH_SIZE = 2000;

    private final DOMDataBroker dataBroker;
    private final DOMSchemaService schemaService;
    private final boolean mustValidate;
    private final DataStoreScope clearScope;
    private final boolean strictDataConsistency;
    private final Consumer<Void> callback;
    private final short maxTraversalDepth;
    private final int listBatchSize;
    private final boolean isBooting;
    @VisibleForTesting
    final ListMultimap<LogicalDatastoreType, File> dataFiles;
    private final Set<YangInstanceIdentifier> createdEmptyParents = new HashSet<>();
    private int batchCount;
    private int writeCount;
    private final Predicate<File> dataFileFilter;

    private static final class DataFileMatcher implements Predicate<File> {
        final Pattern pattern;

        private DataFileMatcher(final String pattern) {
            this.pattern = Pattern.compile(pattern);
        }

        @Override
        public boolean test(final File file) {
            return pattern.matcher(file.getName()).matches();
        }
    }

    public ImportTask(final ImmediateImportInput input, DOMDataBroker domDataBroker,
            final DOMSchemaService schemaService, boolean isBooting, Consumer<Void> callback) {
        this.dataBroker = domDataBroker;
        this.schemaService = schemaService;
        this.mustValidate = input.getCheckModels() != null && input.getCheckModels();
        this.clearScope = input.getClearStores();
        if (input.getImportBatching() != null && input.getImportBatching().getMaxTraversalDepth() != null) {
            this.maxTraversalDepth = input.getImportBatching().getMaxTraversalDepth().shortValue();
        } else {
            this.maxTraversalDepth = DEFAULT_MAX_TRAVERSAL_DEPTH;
        }
        if (input.getImportBatching() != null && input.getImportBatching().getListBatchSize() != null) {
            this.listBatchSize = input.getImportBatching().getListBatchSize().intValue();
        } else {
            this.listBatchSize = DEFAULT_LIST_BATCH_SIZE;
        }
        this.strictDataConsistency = input.getStrictDataConsistency();
        this.isBooting = isBooting;
        this.callback = callback;
        dataFiles = ArrayListMultimap.create(LogicalDatastoreType.values().length, 4);
        this.dataFileFilter = Strings.isNullOrEmpty(input.getFileNameFilter()) ? t -> true
                : new DataFileMatcher(input.getFileNameFilter());
        collectFiles();
        LOG.info("Created import task : {}, collected dump files : {}", input, dataFiles);
    }

    @VisibleForTesting
    int getBatchCount() {
        return batchCount;
    }

    @VisibleForTesting
    int getWriteCount() {
        return writeCount;
    }

    @Override
    @SuppressWarnings("checkstyle:IllegalCatch")
    public ImportOperationResult call() throws Exception {
        callback.accept(null);
        try {
            importInternal();
            return new ImportOperationResultBuilder().setResult(true).build();
        } catch (Exception exception) {
            LOG.error("ImportTask failed", exception);
            return new ImportOperationResultBuilder().setResult(false).setReason(exception.getMessage()).build();
        }
    }

    private void collectFiles() {
        final ListMultimap<LogicalDatastoreType, File> unfiltered = ArrayListMultimap.create(2, 10);
        unfiltered.putAll(Util.collectDataFiles(isBooting));
        for (final LogicalDatastoreType store : unfiltered.asMap().keySet()) {
            final List<File> filtered =
                    unfiltered.asMap().get(store).stream().filter(dataFileFilter).collect(Collectors.toList());
            dataFiles.putAll(store, filtered);
        }
    }

    private InputStream openModelsFile() throws IOException {
        return Files.newInputStream(Util.getModelsFilePath(isBooting));
    }

    private boolean isDataFilePresent(final LogicalDatastoreType store) {
        return dataFiles.containsKey(store) && !dataFiles.get(store).isEmpty();
    }

    private void importInternal()
            throws IOException, ModelsNotAvailableException, InterruptedException, ExecutionException {
        if (mustValidate) {
            if (Util.isModelFilePresent(isBooting)) {
                try (InputStream is = openModelsFile()) {
                    validateModelAvailability(is);
                }
            } else {
                throw new ModelsNotAvailableException("File with models is not present, validation can't be performed");
            }
        } else {
            LOG.warn("Modules availability check is disabled, import may fail if some of models are missing");
        }
        // Import operational data before config data
        for (final LogicalDatastoreType type : Arrays.asList(LogicalDatastoreType.OPERATIONAL,
                LogicalDatastoreType.CONFIGURATION)) {
            importDatastore(type);
        }
        LOG.debug("Batch count for import operation : {}", batchCount);
        LOG.debug("Number of writes used for import operation : {}", writeCount);
    }

    private void importDatastore(final LogicalDatastoreType type)
            throws IOException, InterruptedException, ExecutionException {
        DOMDataTreeReadWriteTransaction rwTrx = null;
        if (strictDataConsistency) {
            rwTrx = dataBroker.newReadWriteTransaction();
        }
        boolean hasDataFile = isDataFilePresent(type);
        if (DataStoreScope.All.equals(clearScope) || DataStoreScope.Data.equals(clearScope) && hasDataFile) {
            removeChildNodes(type, rwTrx);
        }
        if (!hasDataFile) {
            LOG.info("No data file for datastore {}, import skipped", type.name().toLowerCase());
        } else {
            for (final File f : dataFiles.get(type)) {
                try (InputStream is = new FileInputStream(f)) {
                    LOG.info("Loading data into {} datastore from file {}", type.name().toLowerCase(),
                            f.getAbsolutePath());
                    final NormalizedNodeContainerBuilder<?, ?, ?, ?> builder = ImmutableContainerNodeBuilder.create()
                            .withNodeIdentifier(new NodeIdentifier(SchemaContext.NAME));
                    try (NormalizedNodeStreamWriter writer = ImmutableNormalizedNodeStreamWriter.from(builder)) {
                        try (JsonParserStream jsonParser =
                                JsonParserStream.create(writer, CODEC.getShared(schemaService.getGlobalContext()))) {
                            try (JsonReader reader = new JsonReader(new InputStreamReader(is))) {
                                jsonParser.parse(reader);
                                importFromNormalizedNode(rwTrx, type, builder.build());
                            }
                        }
                    }
                }
            }
        }
        if (strictDataConsistency) {
            rwTrx.commit().get();
            batchCount++;
        }
    }

    private void validateModelAvailability(final InputStream inputStream) throws ModelsNotAvailableException {
        final List<Model> md = Util.parseModels(inputStream);
        final Collection<? extends Module> modules = schemaService.getGlobalContext().getModules();
        final Set<Model> missing = new HashSet<>();
        for (final Model m : md) {
            LOG.debug("Checking availability of {}", m);
            boolean found = false;
            for (final Module mod : modules) {
                if (mod.getName().equals(m.getModule()) && mod.getNamespace().toString().equals(m.getNamespace())
                        && Objects.equals(mod.getRevision().map(Revision::toString).orElse(null), m.getRevision())) {
                    found = true;
                }
            }
            if (!found) {
                missing.add(m);
            }
        }
        if (!missing.isEmpty()) {
            throw new ModelsNotAvailableException("Following modules are not available : " + missing);
        }
    }

    private void removeChildNodes(final LogicalDatastoreType type, final DOMDataTreeReadWriteTransaction rwTrx)
            throws InterruptedException, ExecutionException {
        final DOMDataTreeReadWriteTransaction removeTrx;
        if (strictDataConsistency) {
            removeTrx = requireNonNull(rwTrx);
        } else {
            removeTrx = dataBroker.newReadWriteTransaction();
        }
        for (final DataSchemaNode child : schemaService.getGlobalContext().getChildNodes()) {
            if (isInternalObject(child.getQName())) {
                LOG.debug("Skipping removal of internal dataobject : {}", child.getQName());
                continue;
            }
            final YangInstanceIdentifier nodeIID = YangInstanceIdentifier.of(child.getQName());
            if (removeTrx.read(type, nodeIID).get().isPresent()) {
                LOG.debug("Will delete : {}", child.getQName());
                removeTrx.delete(type, nodeIID);
                writeCount++;
            } else {
                LOG.trace("Dataobject not present in {} datastore : {}", type.name().toLowerCase(), child.getQName());
            }
        }
        if (!strictDataConsistency) {
            removeTrx.commit().get();
            batchCount++;
        }
    }

    private static boolean isInternalObject(final QName childQName) {
        return childQName.getLocalName().equals(Util.INTERNAL_LOCAL_NAME);
    }

    private void importFromNormalizedNode(final DOMDataTreeReadWriteTransaction rwTrx, final LogicalDatastoreType type,
            final NormalizedNode<?, ?> data) throws InterruptedException, ExecutionException {
        if (strictDataConsistency) {
            importRootNode(requireNonNull(rwTrx), type, data);
        } else {
            final DOMDataTreeReadWriteTransaction batchTrx = dataBroker.newReadWriteTransaction();
            final boolean commitStatus;
            try {
                commitStatus = importFromNormalizedNodeInBatches(batchTrx, type, data, Collections.emptyList(), 0);
            } catch (final InterruptedException | ExecutionException e) {
                LOG.warn("Exception encountered while importing in batches", e);
                throw e;
            }
            if (!commitStatus) {
                importRootNode(batchTrx, type, data);
            }
            batchTrx.commit().get();
            batchCount++;
        }
    }

    /*
     * Import data at root-node level after excluding data belonging to internal
     * model.
     */
    private void importRootNode(final DOMDataTreeReadWriteTransaction rwTrx, final LogicalDatastoreType type,
            final NormalizedNode<?, ?> data) throws InterruptedException, ExecutionException {
        if (data instanceof NormalizedNodeContainer) {
            for (NormalizedNode<?, ?> child : ((NormalizedNodeContainer<?, ?, ?>) data).getValue()) {
                if (isInternalObject(child.getIdentifier().getNodeType())) {
                    LOG.debug("Skipping import of internal dataobject : {}", child.getIdentifier());
                    continue;
                }
                LOG.debug("Will import : {}", child.getIdentifier());
                rwTrx.put(type, YangInstanceIdentifier.create(child.getIdentifier()), child);
                writeCount++;
            }
        } else {
            throw new IllegalStateException("Root node is not instance of NormalizedNodeContainer");
        }
    }

    /*
     * Iterates through all children looking for lists that exceeds batch size. If
     * such a list is encountered, breaks it down and writes it in batches. If one
     * child is written, all other children are written as well. Returns boolean
     * value indicating if data for the node was written (true) or not (false).
     */
    private boolean importFromNormalizedNodeInBatches(final DOMDataTreeWriteTransaction wrTrx,
            final LogicalDatastoreType type, final NormalizedNode<?, ?> data,
            final Iterable<? extends PathArgument> path, final int depth)
            throws InterruptedException, ExecutionException {
        if (!evaluateNodeForBatchImport(data, depth)) {
            return false;
        }
        LOG.debug("Performing import in batches : {}", data.getIdentifier());

        final Map<NormalizedNode<?, ?>, Boolean> childCommitStatusCache = new HashMap<>();
        final Map<NormalizedNode<?, ?>, Iterable<? extends PathArgument>> childPathCache = new HashMap<>();
        for (final NormalizedNode<?, ?> nnChild : ((NormalizedNodeContainer<?, ?, ?>) data).getValue()) {
            // If we are at root-level, skip importing data for internal model
            if (depth == 0) {
                if (isInternalObject(nnChild.getIdentifier().getNodeType())) {
                    LOG.debug("Skipping import of internal dataobject : {}", nnChild.getIdentifier());
                    continue;
                }
                LOG.debug("Will import : {}", nnChild.getIdentifier());
            }
            if (nnChild instanceof DataContainerChild) {
                final DataContainerChild<? extends PathArgument, ?> dcChild =
                        (DataContainerChild<? extends PathArgument, ?>) nnChild;
                if (dcChild instanceof MapNode) {
                    final MapNode mapNode = (MapNode) dcChild;
                    if (mapNode.getValue().size() > listBatchSize) {
                        writeKeyedListInBatches(type, mapNode, path);
                        childCommitStatusCache.put(nnChild, true);
                        continue;
                    }
                } else if (dcChild instanceof UnkeyedListNode) {
                    final UnkeyedListNode unkeyedListNode = (UnkeyedListNode) dcChild;
                    if (unkeyedListNode.getValue().size() > listBatchSize) {
                        writeUnkeyedListInBatches(type, unkeyedListNode, path);
                        childCommitStatusCache.put(nnChild, true);
                        continue;
                    }
                }
            }

            @SuppressWarnings({"unchecked", "rawtypes"})
            final Iterable<? extends PathArgument> childPath =
                    new ImmutableList.Builder().addAll(path).add(nnChild.getIdentifier()).build();
            final boolean commitStatus =
                    importFromNormalizedNodeInBatches(wrTrx, type, nnChild, childPath, depth + 1);
            childCommitStatusCache.put(nnChild, commitStatus);
            childPathCache.put(nnChild, childPath);
        }

        final int size = childCommitStatusCache.values().stream().collect(Collectors.toSet()).size();
        if (size == 0) {
            // No children present
            return false;
        } else if (size == 1) {
            // All children have same write status
            return childCommitStatusCache.values().stream().findFirst().get();
        }

        // If one child has got written, write all other children as well
        for (final Entry<NormalizedNode<?, ?>, Boolean> entry : childCommitStatusCache.entrySet()) {
            if (!entry.getValue()) {
                wrTrx.merge(type, YangInstanceIdentifier.create(childPathCache.get(entry.getKey())), entry.getKey());
                writeCount++;
            }
        }
        return true;
    }

    /*
     * Determine if the node is eligible for importing in batches, by checking
     * traversal depth and node type.
     */
    private boolean evaluateNodeForBatchImport(final NormalizedNode<?, ?> data, final int depth) {
        if (depth >= maxTraversalDepth) {
            LOG.debug("Max traversal depth exceeded : {}", data.getIdentifier());
            return false;
        }
        if (!(data instanceof NormalizedNodeContainer)) {
            LOG.debug("Not an instance of NormalizedNodeContainer : {}", data.getIdentifier());
            return false;
        }
        return true;
    }

    /*
     * Will be called if size of list exceeds the batch size. It breaks down the
     * list into smaller batches and writes one batch at a time.
     */
    private void writeKeyedListInBatches(final LogicalDatastoreType type, final MapNode mapNode,
            final Iterable<? extends PathArgument> path) throws InterruptedException, ExecutionException {
        @SuppressWarnings({"unchecked", "rawtypes"})
        final YangInstanceIdentifier listIID = YangInstanceIdentifier
                .create(new ImmutableList.Builder().addAll(path).add(mapNode.getIdentifier()).build());

        // Make sure that list parent nodes are present
        ensureParentsByMerge(type, listIID, schemaService.getGlobalContext());

        // Determine if the list is an ordered list
        final DataSchemaContextNode<?> listNode =
                DataSchemaContextTree.from(schemaService.getGlobalContext()).findChild(listIID).get();
        final DataSchemaNode dataSchemaNode = listNode.getDataSchemaNode();
        Preconditions.checkState(dataSchemaNode instanceof ListSchemaNode, dataSchemaNode + " is not a list");
        final boolean ordered;
        if (((ListSchemaNode) dataSchemaNode).isUserOrdered()) {
            ordered = true;
        } else {
            ordered = false;
        }

        final Iterable<List<MapEntryNode>> elementLists = Iterables.partition(mapNode.getValue(), listBatchSize);
        LOG.debug("Importing list {} by splitting into {} batches", mapNode.getIdentifier(),
                Iterables.size(elementLists));
        final Iterator<List<MapEntryNode>> itr = elementLists.iterator();
        while (itr.hasNext()) {
            final List<MapEntryNode> elementList = itr.next();
            final CollectionNodeBuilder<MapEntryNode, ?> newMapNodeBuilder;
            if (ordered) {
                newMapNodeBuilder = ImmutableOrderedMapNodeBuilder.create(listBatchSize);
            } else {
                newMapNodeBuilder = ImmutableMapNodeBuilder.create(listBatchSize);
            }
            newMapNodeBuilder.withNodeIdentifier(mapNode.getIdentifier());
            newMapNodeBuilder.withValue(elementList);

            final DOMDataTreeReadWriteTransaction wrTrx = dataBroker.newReadWriteTransaction();
            wrTrx.merge(type, listIID, newMapNodeBuilder.build());
            writeCount++;
            wrTrx.commit().get();
            batchCount++;
        }
    }

    /*
     * Will be called if size of unkeyed list exceeds the batch size. It breaks down
     * the list into smaller batches and writes one batch at a time.
     */
    private void writeUnkeyedListInBatches(final LogicalDatastoreType type, final UnkeyedListNode unkeyedListNode,
            final Iterable<? extends PathArgument> path) throws InterruptedException, ExecutionException {
        @SuppressWarnings({"unchecked", "rawtypes"})
        final YangInstanceIdentifier listIID = YangInstanceIdentifier
                .create(new ImmutableList.Builder().addAll(path).add(unkeyedListNode.getIdentifier()).build());

        // Make sure that list parent nodes are present
        ensureParentsByMerge(type, listIID, schemaService.getGlobalContext());

        final Iterable<List<UnkeyedListEntryNode>> elementLists =
                Iterables.partition(unkeyedListNode.getValue(), listBatchSize);
        LOG.debug("Importing unkeyed list {} by splitting into {} batches", unkeyedListNode.getIdentifier(),
                Iterables.size(elementLists));
        final Iterator<List<UnkeyedListEntryNode>> itr = elementLists.iterator();
        while (itr.hasNext()) {
            final List<UnkeyedListEntryNode> elementList = itr.next();
            final CollectionNodeBuilder<UnkeyedListEntryNode, ?> newUnkeyedListNodeBuilder =
                    ImmutableUnkeyedListNodeBuilder.create(listBatchSize);
            newUnkeyedListNodeBuilder.withNodeIdentifier(unkeyedListNode.getIdentifier());
            newUnkeyedListNodeBuilder.withValue(elementList);

            final DOMDataTreeReadWriteTransaction wrTrx = dataBroker.newReadWriteTransaction();
            wrTrx.merge(type, listIID, newUnkeyedListNodeBuilder.build());
            writeCount++;
            wrTrx.commit().get();
            batchCount++;
        }
    }

    /*
     * Creates parent node for the list if missing.
     */
    private void ensureParentsByMerge(final LogicalDatastoreType store, final YangInstanceIdentifier normalizedPath,
            final SchemaContext schemaContext) throws InterruptedException, ExecutionException {
        final List<PathArgument> normalizedPathWithoutChildArgs = new ArrayList<>();
        YangInstanceIdentifier rootNormalizedPath = null;

        final Iterator<PathArgument> itr = normalizedPath.getPathArguments().iterator();

        while (itr.hasNext()) {
            final PathArgument pathArgument = itr.next();
            if (rootNormalizedPath == null) {
                rootNormalizedPath = YangInstanceIdentifier.create(pathArgument);
            }
            if (itr.hasNext()) {
                normalizedPathWithoutChildArgs.add(pathArgument);
            }
        }

        if (normalizedPathWithoutChildArgs.isEmpty()) {
            return;
        }

        Preconditions.checkArgument(rootNormalizedPath != null, "Empty path received");
        if (createdEmptyParents.contains(rootNormalizedPath)) {
            LOG.debug("Empty parent already exists at : {}", rootNormalizedPath);
            return;
        }

        final NormalizedNode<?, ?> parentStructure = ImmutableNodes.fromInstanceId(schemaContext,
                YangInstanceIdentifier.create(normalizedPathWithoutChildArgs));

        LOG.debug("Creating empty parent at {} : {}", rootNormalizedPath, parentStructure);
        final DOMDataTreeReadWriteTransaction rwTrx = dataBroker.newReadWriteTransaction();
        rwTrx.merge(store, rootNormalizedPath, parentStructure);
        writeCount++;
        rwTrx.commit().get();
        batchCount++;
        createdEmptyParents.add(rootNormalizedPath);
    }
}
