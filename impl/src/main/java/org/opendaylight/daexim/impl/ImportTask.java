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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.ReadFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.controller.md.sal.dom.api.DOMDataBroker;
import org.opendaylight.controller.md.sal.dom.api.DOMDataReadWriteTransaction;
import org.opendaylight.controller.sal.core.api.model.SchemaService;
import org.opendaylight.daexim.impl.model.internal.Model;
import org.opendaylight.daexim.impl.model.internal.ModelsNotAvailableException;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.ImportOperationResult;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.internal.rev160921.ImportOperationResultBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataStoreScope;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInput;
import org.opendaylight.yangtools.yang.common.SimpleDateFormatUtil;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier.PathArgument;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNode;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNodeContainer;
import org.opendaylight.yangtools.yang.data.api.schema.stream.NormalizedNodeStreamWriter;
import org.opendaylight.yangtools.yang.data.codec.gson.JsonParserStream;
import org.opendaylight.yangtools.yang.data.impl.schema.ImmutableNormalizedNodeStreamWriter;
import org.opendaylight.yangtools.yang.data.impl.schema.builder.api.NormalizedNodeContainerBuilder;
import org.opendaylight.yangtools.yang.data.impl.schema.builder.impl.ImmutableContainerNodeBuilder;
import org.opendaylight.yangtools.yang.model.api.Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import com.google.gson.stream.JsonReader;

public class ImportTask implements Callable<ImportOperationResult> {
    private static final Logger LOG = LoggerFactory.getLogger(ImportTask.class);
    private final DOMDataBroker dataBroker;
    private final SchemaService schemaService;
    private final boolean mustValidate;
    private final DataStoreScope clearScope;
    private final Callback callback;
    @VisibleForTesting
    final ListMultimap<LogicalDatastoreType, File> dataFiles;

    public ImportTask(final ImmediateImportInput input, DOMDataBroker domDataBroker, final SchemaService schemaService,
            Callback callback) {
        this.dataBroker = domDataBroker;
        this.schemaService = schemaService;
        this.mustValidate = (input.isCheckModels() != null && input.isCheckModels());
        this.clearScope = input.getClearStores();
        this.callback = callback;
        dataFiles = ArrayListMultimap.create(LogicalDatastoreType.values().length, 4);
        collectFiles();
        LOG.info("Created import task : {}, collected dump files : {}", input, dataFiles);
    }

    @Override
    public ImportOperationResult call() throws Exception {
        callback.call();
        try {
            importInternal();
            return new ImportOperationResultBuilder().setResult(true).build();
        } catch (Exception e) {
            return new ImportOperationResultBuilder().setResult(false).setReason(e.getMessage()).build();
        }
    }

    private void collectFiles() {
        dataFiles.putAll(Util.collectDataFiles());
    }

    private static InputStream openModelsFile() throws IOException {
        return Files.newInputStream(Util.getModelsFilePath());
    }

    private boolean isDataFilePresent(final LogicalDatastoreType store) {
        return (dataFiles.containsKey(store) && !dataFiles.get(store).isEmpty());
    }

    private void importInternal()
            throws IOException, ModelsNotAvailableException, TransactionCommitFailedException, ReadFailedException {
        if (mustValidate) {
            if (Util.isModelFilePresent()) {
                try (final InputStream is = openModelsFile()) {
                    validateModelAvailability(is);
                }
            } else {
                throw new ModelsNotAvailableException("File with models is not present, validation can't be performed");
            }
        } else {
            LOG.warn("Modules availability check is disabled, import may fail if some of models are missing");
        }
        for (final LogicalDatastoreType type : LogicalDatastoreType.values()) {
            importDatastore(type);
        }
    }

    private void importDatastore(final LogicalDatastoreType type)
            throws ReadFailedException, TransactionCommitFailedException, IOException {
        final DOMDataReadWriteTransaction rwTrx = dataBroker.newReadWriteTransaction();
        boolean hasDataFile = isDataFilePresent(type);
        if (DataStoreScope.All.equals(clearScope) || (DataStoreScope.Data.equals(clearScope) && hasDataFile)) {
            removeChildNodes(type, rwTrx);
        }
        if (!hasDataFile) {
            LOG.info("No data file for datastore {}, import skipped", type.name().toLowerCase());
        } else {
            for (final File f : dataFiles.get(type)) {
                try (final InputStream is = new FileInputStream(f)) {
                    LOG.info("Loading data into {} datastore from file {}", type.name().toLowerCase(),
                            f.getAbsolutePath());
                    final NormalizedNodeContainerBuilder<?, ?, ?, ?> builder = ImmutableContainerNodeBuilder.create()
                            .withNodeIdentifier(new YangInstanceIdentifier.NodeIdentifier(
                                    schemaService.getGlobalContext().getQName()));

                    final NormalizedNodeStreamWriter writer = ImmutableNormalizedNodeStreamWriter.from(builder);
                    schemaService.getGlobalContext().getQName();
                    final JsonParserStream jsonParser = JsonParserStream.create(writer,
                            schemaService.getGlobalContext());
                    final JsonReader reader = new JsonReader(new InputStreamReader(is));
                    jsonParser.parse(reader);
                    importFromNormalizedNode(rwTrx, type, builder.build());
                    reader.close();
                }
            }
        }
        rwTrx.submit().checkedGet();
    }

    private void validateModelAvailability(final InputStream inputStream)
            throws IOException, ModelsNotAvailableException {
        final List<Model> md = Util.parseModels(inputStream);
        final Set<Module> modules = schemaService.getGlobalContext().getModules();
        final Set<Model> missing = Sets.newHashSet();
        for (final Model m : md) {
            LOG.debug("Checking availability of {}", m);
            boolean found = false;
            for (final Module mod : modules) {
                if (mod.getName().equals(m.getModule()) && mod.getNamespace().toString().equals(m.getNamespace())
                        && SimpleDateFormatUtil.getRevisionFormat().format(mod.getRevision()).equals(m.getRevision())) {
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

    private void removeChildNodes(final LogicalDatastoreType type, final DOMDataReadWriteTransaction rwTrx)
            throws ReadFailedException {
        final Optional<NormalizedNode<?, ?>> rootNode = rwTrx.read(type, YangInstanceIdentifier.EMPTY).checkedGet();
        if (rootNode.isPresent()) {
            final NormalizedNode<?, ?> nn = rootNode.get();
            if (nn instanceof NormalizedNodeContainer) {
                @SuppressWarnings("unchecked")
                final NormalizedNodeContainer<? extends PathArgument, ? extends PathArgument, ? extends NormalizedNode<PathArgument, ?>> nnContainer = ((NormalizedNodeContainer<? extends PathArgument, ? extends PathArgument, ? extends NormalizedNode<PathArgument, ?>>) nn);
                for (final NormalizedNode<PathArgument, ?> child : nnContainer.getValue()) {
                    if (isInternalObject(child)) {
                        LOG.debug("Skipping removal of internal dataobject : {}", child.getIdentifier());
                        continue;
                    }
                    LOG.debug("Will delete : {}", child.getIdentifier());
                    rwTrx.delete(type, YangInstanceIdentifier.create(child.getIdentifier()));
                }
            } else {
                LOG.warn("Root node is not instance of NormalizedNodeContainer, delete skipped");
            }
        }
    }

    private boolean isInternalObject(NormalizedNode<YangInstanceIdentifier.PathArgument, ?> child) {
        return child.getIdentifier().getNodeType().getLocalName().equals(Util.INTERNAL_LOCAL_NAME);
    }

    private void importFromNormalizedNode(final DOMDataReadWriteTransaction rwTrx, final LogicalDatastoreType type,
            final NormalizedNode<?, ?> data) throws TransactionCommitFailedException, ReadFailedException {
        if (data instanceof NormalizedNodeContainer) {
            @SuppressWarnings("unchecked")
            final NormalizedNodeContainer<? extends PathArgument, ? extends PathArgument, ? extends NormalizedNode<YangInstanceIdentifier.PathArgument, ?>> nnContainer = ((NormalizedNodeContainer<? extends PathArgument, ? extends PathArgument, ? extends NormalizedNode<YangInstanceIdentifier.PathArgument, ?>>) data);
            final Collection<? extends NormalizedNode<YangInstanceIdentifier.PathArgument, ?>> children = nnContainer
                    .getValue();
            for (NormalizedNode<YangInstanceIdentifier.PathArgument, ?> child : children) {
                if (isInternalObject(child)) {
                    LOG.debug("Skipping import of internal dataobject : {}", child.getIdentifier());
                    continue;
                }
                LOG.debug("Will import : {}", child.getIdentifier());
                rwTrx.put(type, YangInstanceIdentifier.create(child.getIdentifier()), child);
            }
        } else {
            throw new IllegalStateException("Root node is not instance of NormalizedNodeContainer");
        }
    }
}
