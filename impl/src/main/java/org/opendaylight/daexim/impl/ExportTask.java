/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.stream.JsonWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.opendaylight.daexim.impl.model.internal.Model;
import org.opendaylight.mdsal.common.api.LogicalDatastoreType;
import org.opendaylight.mdsal.dom.api.DOMDataBroker;
import org.opendaylight.mdsal.dom.api.DOMDataTreeReadTransaction;
import org.opendaylight.mdsal.dom.api.DOMSchemaService;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev130715.YangIdentifier;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataStore;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModules;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModules.ModuleName;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModulesBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModulesModuleNameBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.inclusions.IncludedModules;
import org.opendaylight.yangtools.yang.common.QName;
import org.opendaylight.yangtools.yang.common.Revision;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier.NodeIdentifier;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNode;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNodeContainer;
import org.opendaylight.yangtools.yang.data.api.schema.stream.NormalizedNodeWriter;
import org.opendaylight.yangtools.yang.data.codec.gson.JSONCodecFactory;
import org.opendaylight.yangtools.yang.data.codec.gson.JSONCodecFactorySupplier;
import org.opendaylight.yangtools.yang.data.codec.gson.JSONNormalizedNodeStreamWriter;
import org.opendaylight.yangtools.yang.model.api.DataSchemaNode;
import org.opendaylight.yangtools.yang.model.api.Module;
import org.opendaylight.yangtools.yang.model.api.SchemaPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportTask implements Callable<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(ExportTask.class);
    private static final JSONCodecFactorySupplier CODEC = JSONCodecFactorySupplier.DRAFT_LHOTKA_NETMOD_YANG_JSON_02;
    private static final String FIELD_MODULE = "module";
    private static final String FIELD_NAMESPACE = "namespace";
    private static final String FIELD_REVISION = "revision-date";

    private final DOMDataBroker domDataBroker;
    private final JSONCodecFactory codecFactory;
    private final DOMSchemaService schemaService;
    private final List<IncludedModules> includedModules;
    private final List<ExcludedModules> excludedModules;
    private final Consumer<Void> callback;
    private final Set<LogicalDatastoreType> excludedDss = Sets.newHashSet();
    private final boolean strictDataConsistency;
    private final boolean isPerModuleExport;

    public ExportTask(final List<IncludedModules> includedModules, final List<ExcludedModules> excludedModules,
            final boolean strictDataConsistency, final boolean isPerModuleExport,
            final DOMDataBroker domDataBroker, final DOMSchemaService schemaService, final Consumer<Void> callback) {
        this.domDataBroker = domDataBroker;
        this.codecFactory = CODEC.getShared(schemaService.getGlobalContext());
        this.schemaService = schemaService;
        this.includedModules = includedModules != null ? includedModules : Collections.emptyList();
        this.excludedModules = ensureSelfExclusion(excludedModules);
        for (final ExcludedModules em : this.excludedModules) {
            if (em.getModuleName().getWildcardStar() != null
                    && ExcludedModulesModuleNameBuilder.STAR.equals(em.getModuleName().getWildcardStar().getValue())) {
                excludedDss.add(Util.storeTypeFromName(getDataStoreFromExclusion(em).toLowerCase()));
            }
        }
        this.callback = callback;
        this.strictDataConsistency = strictDataConsistency;
        this.isPerModuleExport = isPerModuleExport;
    }

    /*
     * Exclude ourself from dump
     */
    private List<ExcludedModules> ensureSelfExclusion(List<ExcludedModules> others) {
        final List<ExcludedModules> self = Lists
                .newArrayList(new ExcludedModulesBuilder().setDataStore(new DataStore("operational"))
                        .setModuleName(new ModuleName(new YangIdentifier(Util.INTERNAL_MODULE_NAME))).build());
        if (others != null) {
            self.addAll(others);
        }
        return self;
    }

    /*
     * Cache module name mapping for efficient lookups
     */
    private final LoadingCache<String, Optional<Model>> moduleCache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Optional<Model>>() {
                @Override
                public Optional<Model> load(String moduleName) throws Exception {
                    final Set<Module> mods = schemaService.getGlobalContext().getModules();
                    for (final Module m : mods) {
                        if (m.getName().equals(moduleName)) {
                            final Model model = new Model();
                            model.setModule(moduleName);
                            model.setRevision(m.getRevision().map(Revision::toString).orElse(null));
                            model.setNamespace(m.getNamespace().toString());
                            return Optional.of(model);
                        }
                    }
                    return Optional.empty();
                }
            });

    // JsonWriter's close() will close new FileWriter
    private JsonWriter createWriter(LogicalDatastoreType type, boolean isModules) throws IOException {
        final File filePath = isModules ? Util.getModelsFilePath(false).toFile()
                : Util.getDaeximFilePath(false, type).toFile();
        LOG.info("Creating JSON file : {}", filePath);
        return new JsonWriter(new FileWriter(filePath));
    }

    private JsonWriter createPerModuleWriter(LogicalDatastoreType store, NodeIdentifier ni) throws IOException {
        final StringBuilder fileName = new StringBuilder();
        fileName.append(Util.FILE_PREFIX);
        fileName.append(Util.storeNameByType(store));
        java.util.Optional<Module> mod = schemaService.getGlobalContext().findModule(ni.getNodeType().getNamespace(),
                ni.getNodeType().getRevision());
        if (mod.isPresent()) {
            fileName.append('_');
            fileName.append(mod.get().getName());
            appendRevision(fileName, mod.get().getRevision());
        }
        fileName.append(Util.FILE_SUFFIX);
        final Path filePath = Paths.get(Util.getDaeximDir(false), fileName.toString());
        LOG.info("Creating JSON file : {}", filePath);
        return new JsonWriter(new FileWriter(filePath.toFile()));
    }

    private void appendRevision(final StringBuilder fileName, java.util.Optional<Revision> optional) {
        if (optional.isPresent()) {
            fileName.append('@');
            fileName.append(optional.get().toString());
        }
    }

    private void writeEmptyStore(LogicalDatastoreType type) throws IOException {
        try (JsonWriter writer = createWriter(type, false)) {
            writer.beginObject();
            writer.endObject();
            writer.flush();
        }
    }

    private void writeStore(LogicalDatastoreType type) throws IOException, InterruptedException, ExecutionException {
        final Collection<NormalizedNode<?, ?>> nodes = readDatastore(type);
        LOG.debug("Number of nodes for export after handling inclusions/exclusions : {}", nodes.size());
        if (isPerModuleExport) {
            for (NormalizedNode<?, ?> nn : nodes) {
                writeModuleData(nn, createPerModuleWriter(type, (NodeIdentifier) nn.getIdentifier()));
            }
        } else {
            try (JsonWriter jsonWriter = createWriter(type, false)) {
                writeData(nodes, jsonWriter);
            }
        }
    }

    private Collection<NormalizedNode<?, ?>> readDatastore(final LogicalDatastoreType type)
            throws InterruptedException, ExecutionException {
        if (strictDataConsistency) {
            return readDatastoreOneShot(type);
        } else {
            return readDatastorePerChild(type);
        }
    }

    /*
     * Read datastore in one shot and then handle inclusions/exclusions
     */
    private Collection<NormalizedNode<?, ?>> readDatastoreOneShot(final LogicalDatastoreType type)
            throws InterruptedException, ExecutionException {
        final Optional<NormalizedNode<?, ?>> opt = getRootNode(type);
        if (!opt.isPresent()) {
            throw new IllegalStateException("Root node is not present");
        }
        final NormalizedNode<?, ?> root = opt.get();
        if (root instanceof NormalizedNodeContainer) {
            @SuppressWarnings("unchecked")
            final Collection<NormalizedNode<?, ?>> nodes = (Collection<NormalizedNode<?, ?>>) root.getValue();
            return nodes.stream().filter(node -> isIncludedOrNotExcluded(type, node.getIdentifier().getNodeType()))
                    .collect(Collectors.toSet());
        } else {
            throw new IllegalStateException("Root node is not instance of NormalizedNodeContainer");
        }
    }

    /*
     * Handle inclusions/exclusions and then read datastore one node at a time
     */
    private Collection<NormalizedNode<?, ?>> readDatastorePerChild(final LogicalDatastoreType type)
            throws InterruptedException, ExecutionException {
        final Collection<NormalizedNode<?, ?>> nodes = Sets.newHashSet();
        for (final DataSchemaNode schemaNode : schemaService.getGlobalContext().getChildNodes()) {
            if (!isIncludedOrNotExcluded(type, schemaNode.getQName())) {
                continue;
            }
            LOG.trace("Handling child node : {}", schemaNode.getQName());
            final Optional<NormalizedNode<?, ?>> opt = getNode(type, YangInstanceIdentifier.of(schemaNode.getQName()));
            if (!opt.isPresent()) {
                LOG.trace("Data for child is not present : {}", schemaNode.getQName());
                continue;
            }
            final NormalizedNode<?, ?> nn = opt.get();
            if (!(nn instanceof NormalizedNodeContainer)) {
                LOG.warn("Data for child is not an instance of NormalizedNodeContainer : {}", schemaNode.getQName());
                continue;
            }
            nodes.add(nn);
        }
        return nodes;
    }

    private Optional<NormalizedNode<?, ?>> getRootNode(final LogicalDatastoreType type)
            throws InterruptedException, ExecutionException {
        return getNode(type, YangInstanceIdentifier.empty());
    }

    private Optional<NormalizedNode<?, ?>> getNode(final LogicalDatastoreType type,
            final YangInstanceIdentifier nodeIID) throws InterruptedException, ExecutionException {
        final DOMDataTreeReadTransaction roTrx = domDataBroker.newReadOnlyTransaction();
        try {
            LOG.trace("Reading data for node : {}", nodeIID);
            return roTrx.read(type, nodeIID).get();
        } finally {
            roTrx.close();
        }
    }

    @Override
    public Void call() throws Exception {
        callback.accept(null);
        writeModules(createWriter(null, true));

        for (final LogicalDatastoreType type : LogicalDatastoreType.values()) {
            if (excludedDss.contains(type)) {
                LOG.info("Datastore excluded : {}", type.name().toLowerCase());
                writeEmptyStore(type);
            } else {
                writeStore(type);
            }
        }
        return null;
    }

    private void writeProperty(JsonWriter writer, String name, String value) throws IOException {
        writer.name(name);
        writer.value(value);
    }

    private void writeModules(final JsonWriter jsonWriter) throws IOException {
        jsonWriter.beginArray();

        final Set<Module> modules = schemaService.getGlobalContext().getModules();

        for (final Module mod : modules) {
            jsonWriter.beginObject();
            writeProperty(jsonWriter, FIELD_MODULE, mod.getName());
            writeProperty(jsonWriter, FIELD_NAMESPACE, mod.getNamespace().toString());
            writeProperty(jsonWriter, FIELD_REVISION, mod.getRevision().map(Revision::toString).orElse(null));
            jsonWriter.endObject();
        }

        jsonWriter.endArray();
        jsonWriter.flush();
        jsonWriter.close();
    }

    private void writeModuleData(final NormalizedNode<?, ?> node, final JsonWriter jsonWriter) throws IOException {
        jsonWriter.beginObject();
        try (NormalizedNodeWriter nnWriter = NormalizedNodeWriter.forStreamWriter(
                JSONNormalizedNodeStreamWriter.createNestedWriter(codecFactory, SchemaPath.ROOT, null, jsonWriter),
                true)) {
            nnWriter.write(node);
            nnWriter.flush();
            jsonWriter.endObject();
        }
    }

    private void writeData(final Collection<? extends NormalizedNode<?, ?>> children, final JsonWriter jsonWriter)
            throws IOException {

        jsonWriter.beginObject();
        try (NormalizedNodeWriter nnWriter = NormalizedNodeWriter.forStreamWriter(
                JSONNormalizedNodeStreamWriter.createNestedWriter(codecFactory, SchemaPath.ROOT, null, jsonWriter),
                true)) {

            for (final NormalizedNode<?, ?> child : children) {
                nnWriter.write(child);
                nnWriter.flush();
            }
            jsonWriter.endObject();
        }
    }

    private boolean isIncludedOrNotExcluded(final LogicalDatastoreType type, final QName nodeQName) {
        final boolean selected;
        if (includedModules.isEmpty()) {
            selected = !isExcluded(type, nodeQName);
        } else {
            selected = !isExcluded(type, nodeQName) && isIncluded(type, nodeQName);
        }
        if (!selected) {
            LOG.info("Node excluded from export : {}", nodeQName);
        }
        return selected;
    }

    private boolean isIncluded(final LogicalDatastoreType type, final QName nodeQName) {
        for (final IncludedModules incl : includedModules) {
            LOG.debug("Checking for inclusion of {} in {} against {}", nodeQName, type, incl);
            if (!Util.storeNameByType(type).equalsIgnoreCase(getDataStoreFromInclusion(incl))) {
                // The datastore type being written does not match the one in
                // the include list, so try the next item in exclude list.
                continue;
            }
            final Optional<Model> mod = moduleCache.getUnchecked(incl.key().getModuleName().getValue());
            // SchemaService found the module being excluded. Compare it to the node being
            // written, matching only the namespace and ignoring the revision.
            if (mod.isPresent() && mod.get().getNamespace().equals(nodeQName.getNamespace().toString())) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    boolean isExcluded(final LogicalDatastoreType type, final QName nodeQName) {
        for (final ExcludedModules excl : excludedModules) {
            LOG.debug("Checking for exclusion of {} in {} against {}", nodeQName, type, excl);
            if (!Util.storeNameByType(type).equalsIgnoreCase(getDataStoreFromExclusion(excl))) {
                // The datastore type being written does not match the one in
                // the exclude list, so try the next item in exclude list.
                continue;
            }
            final Optional<Model> mod = moduleCache
                    .getUnchecked(excl.key().getModuleName().getYangIdentifier().getValue());
            // SchemaService found the module being excluded. Compare it to the node being
            // written, matching only the namespace and ignoring the revision.
            if (mod.isPresent() && mod.get().getNamespace().equals(nodeQName.getNamespace().toString())) {
                return true;
            }
        }
        return false;
    }

    private String getDataStoreFromInclusion(IncludedModules incl) {
        return Strings.isNullOrEmpty(incl.getDataStore().getString()) ? incl.getDataStore().getEnumeration().getName()
                : incl.getDataStore().getString();
    }

    private String getDataStoreFromExclusion(ExcludedModules excl) {
        return Strings.isNullOrEmpty(excl.getDataStore().getString()) ? excl.getDataStore().getEnumeration().getName()
                : excl.getDataStore().getString();
    }
}
