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
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.stream.JsonWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.WillClose;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.ReadFailedException;
import org.opendaylight.controller.md.sal.dom.api.DOMDataBroker;
import org.opendaylight.controller.md.sal.dom.api.DOMDataReadOnlyTransaction;
import org.opendaylight.controller.sal.core.api.model.SchemaService;
import org.opendaylight.daexim.impl.model.internal.Model;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev130715.YangIdentifier;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataStore;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModules;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModules.ModuleName;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModulesBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.exclusions.ExcludedModulesModuleNameBuilder;
import org.opendaylight.yangtools.yang.common.QName;
import org.opendaylight.yangtools.yang.common.SimpleDateFormatUtil;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier.PathArgument;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNode;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNodeContainer;
import org.opendaylight.yangtools.yang.data.api.schema.stream.NormalizedNodeWriter;
import org.opendaylight.yangtools.yang.data.codec.gson.JSONCodecFactory;
import org.opendaylight.yangtools.yang.data.codec.gson.JSONNormalizedNodeStreamWriter;
import org.opendaylight.yangtools.yang.model.api.Module;
import org.opendaylight.yangtools.yang.model.api.SchemaPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportTask implements Callable<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(ExportTask.class);

    private static final String FIELD_MODULE = "module";
    private static final String FIELD_NAMESPACE = "namespace";
    private static final String FIELD_REVISION = "revision-date";

    private final DOMDataBroker domDataBroker;
    private final JSONCodecFactory codecFactory;
    private final SchemaService schemaService;
    private final List<ExcludedModules> excludedModules;
    private final Callback callback;
    private final Set<LogicalDatastoreType> excludedDss = Sets.newHashSet();

    public ExportTask(final List<ExcludedModules> excludedModules, final DOMDataBroker domDataBroker,
            final SchemaService schemaService, Callback callback) {
        this.domDataBroker = domDataBroker;
        this.codecFactory = JSONCodecFactory.getShared(schemaService.getGlobalContext());
        this.schemaService = schemaService;
        this.excludedModules = ensureSelfExclusion(excludedModules);
        for (final ExcludedModules em : this.excludedModules) {
            if (em.getModuleName().getWildcardStar() != null
                    && ExcludedModulesModuleNameBuilder.STAR.equals(em.getModuleName().getWildcardStar().getValue())) {
                excludedDss.add(Util.storeTypeFromName(getDataStoreFromExclusion(em).toLowerCase()));
            }
        }
        this.callback = callback;
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
                            model.setRevision(SimpleDateFormatUtil.getRevisionFormat().format(m.getRevision()));
                            model.setNamespace(m.getNamespace().toString());
                            return Optional.of(model);
                        }
                    }
                    return Optional.absent();
                }
            });

    private Optional<NormalizedNode<?, ?>> getRootNode(LogicalDatastoreType type) throws ReadFailedException {
        final DOMDataReadOnlyTransaction roTrx = domDataBroker.newReadOnlyTransaction();
        try {
            return roTrx.read(type, YangInstanceIdentifier.EMPTY).checkedGet();
        } finally {
            roTrx.close();
        }
    }

    private JsonWriter createWriter(LogicalDatastoreType type, boolean isModules) throws IOException {
        final String filePath = isModules ? Util.getModelsFilePath().toFile().getAbsolutePath()
                : Util.getDaeximFilePath(type).toFile().getAbsolutePath();
        LOG.info("Creating JSON file : {}", filePath);
        return new JsonWriter(new FileWriter(filePath));
    }

    private void writeEmptyStore(LogicalDatastoreType type) throws IOException {
        try (JsonWriter writer = createWriter(type, false)) {
            writer.beginObject();
            writer.endObject();
            writer.flush();
        }
    }

    private void writeStore(LogicalDatastoreType type) throws IOException, ReadFailedException {
        final Optional<NormalizedNode<?, ?>> opt = getRootNode(type);
        if (!opt.isPresent()) {
            throw new IllegalStateException("Root node is not present");
        }
        final NormalizedNode<?, ?> nn = opt.get();
        if (nn instanceof NormalizedNodeContainer) {
            @SuppressWarnings("unchecked")
            NormalizedNodeContainer<? extends PathArgument, ? extends PathArgument, ? extends NormalizedNode<?, ?>>
                nnContainer = (NormalizedNodeContainer<? extends PathArgument, ? extends PathArgument,
                        ? extends NormalizedNode<?, ?>>) nn;
            try (JsonWriter jsonWriter = createWriter(type, false)) {
                writeData(type, nnContainer.getValue(), jsonWriter);
            }
        } else {
            throw new IllegalStateException("Root node is not instance of NormalizedNodeContainer");
        }
    }

    @Override
    public Void call() throws Exception {
        callback.call();
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

    private void writeModules(final @WillClose JsonWriter jsonWriter) throws IOException {
        jsonWriter.beginArray();

        final Set<Module> modules = schemaService.getGlobalContext().getModules();

        for (final Module mod : modules) {
            jsonWriter.beginObject();
            writeProperty(jsonWriter, FIELD_MODULE, mod.getName());
            writeProperty(jsonWriter, FIELD_NAMESPACE, mod.getNamespace().toString());
            writeProperty(jsonWriter, FIELD_REVISION,
                    SimpleDateFormatUtil.getRevisionFormat().format(mod.getRevision()));
            jsonWriter.endObject();
        }

        jsonWriter.endArray();
        jsonWriter.flush();
        jsonWriter.close();
    }

    private void writeData(final LogicalDatastoreType type, final Collection<? extends NormalizedNode<?, ?>> children,
            final JsonWriter jsonWriter) throws IOException {

        jsonWriter.beginObject();
        final NormalizedNodeWriter nnWriter = NormalizedNodeWriter.forStreamWriter(
                JSONNormalizedNodeStreamWriter.createNestedWriter(codecFactory, SchemaPath.ROOT, null, jsonWriter),
                true);

        for (final NormalizedNode<?, ?> child : children) {
            if (!isExcluded(type, child)) {
                nnWriter.write(child);
                nnWriter.flush();
            } else {
                LOG.info("Node excluded : {}", child.getIdentifier());
            }
        }
        jsonWriter.endObject();
    }

    private String getDataStoreFromExclusion(ExcludedModules excl) {
        return Strings.isNullOrEmpty(excl.getDataStore().getString()) ? excl.getDataStore().getEnumeration().getName()
                : excl.getDataStore().getString();
    }

    @VisibleForTesting
    boolean isExcluded(final LogicalDatastoreType type, final NormalizedNode<?, ?> node) {
        for (final ExcludedModules excl : excludedModules) {
            LOG.debug("Checking for exclusion of {} in {} against {}", node, type, excl);
            if (!Util.storeNameByType(type).equalsIgnoreCase(getDataStoreFromExclusion(excl))) {
                // The datastore type being written does not match the one in
                // exclude list.
                // Try the next item in exclude list.
                continue;
            }
            final Optional<Model> mod = moduleCache
                    .getUnchecked(excl.getKey().getModuleName().getYangIdentifier().getValue());
            if (mod.isPresent()) {
                // SchemaService found the module being excluded. Compare it to
                // the node being written.
                // Match only the namespace and ignore the revision.
                final QName nodeType = node.getIdentifier().getNodeType();
                if (mod.get().getNamespace().equals(nodeType.getNamespace().toString())) {
                    return true;
                }
            }
        }
        return false;
    }
}
