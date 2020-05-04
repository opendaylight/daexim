/*
 * Copyright (c) 2018 Lumina Networks, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.mdsal.binding.api.WriteTransaction;
import org.opendaylight.mdsal.binding.dom.adapter.test.AbstractDataBrokerTest;
import org.opendaylight.mdsal.common.api.LogicalDatastoreType;
import org.opendaylight.mdsal.dom.api.DOMSchemaService;
import org.opendaylight.yang.gen.v1.testa.rev160912.Data2;
import org.opendaylight.yang.gen.v1.testa.rev160912.Data2Builder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopologyBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.TopologyBuilder;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for split-per-module feature.
 *
 * @author <a href="mailto:richard.kosegi@gmail.com">Richard Kosegi</a>
 * @since Jul 4, 2018
 */
public class PerModuleExportTest extends AbstractDataBrokerTest {
    private static final Logger LOG = LoggerFactory.getLogger(PerModuleExportTest.class);
    private Path tmpDir;
    private DOMSchemaService schemaService;
    @SuppressWarnings("unchecked")
    private Consumer<Void> callback = mock(Consumer.class);

    @Before
    public void setUp() throws Exception {
        tmpDir = Files.createTempDirectory("daexim-test-tmp");
        Files.createDirectory(tmpDir.resolve(Util.DAEXIM_DIR));
        System.setProperty("karaf.home", tmpDir.toString());
        LOG.info("Created temp directory : {}", tmpDir);
        schemaService = mock(DOMSchemaService.class);
        doReturn(getSchemaContext()).when(schemaService).getGlobalContext();
    }

    @After
    public void tearDown() throws IOException {
        Files.walkFileTree(tmpDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }

        });
        reset(schemaService);
    }

    @Test
    public void test() throws Exception {
        // 1, populate datastore
        writeDataToRoot(InstanceIdentifier.create(NetworkTopology.class), new NetworkTopologyBuilder()
                .setTopology(
                        Lists.newArrayList(new TopologyBuilder().setTopologyId(TestBackupData.TOPOLOGY_ID).build()))
                .build());
        writeDataToRoot(InstanceIdentifier.create(Data2.class), new Data2Builder().setLeaf1("A").build());
        // 2, perform export
        ExportTask et = new ExportTask(null, null, true, true, getDomBroker(), schemaService, callback);
        et.call();
        // 3, ensure per-module files exists
        String[] jsonFiles = tmpDir.resolve(Util.DAEXIM_DIR)
                .toFile()
                .list((dir, name) -> name.endsWith(Util.FILE_SUFFIX));
        assertTrue(jsonFiles.length >= 3);
    }

    private <D extends DataObject> void writeDataToRoot(InstanceIdentifier<D> ii, D dataObject)
            throws InterruptedException, ExecutionException {
        final WriteTransaction wrTrx = getDataBroker().newWriteOnlyTransaction();
        wrTrx.put(LogicalDatastoreType.OPERATIONAL, ii, dataObject);
        wrTrx.commit().get();
    }
}
