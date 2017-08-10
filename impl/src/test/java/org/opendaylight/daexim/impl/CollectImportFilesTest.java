/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ListMultimap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.matchers.EndsWith;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.dom.api.DOMDataBroker;
import org.opendaylight.controller.sal.core.api.model.SchemaService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.ImmediateImportInputBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Goal of this test is to ensure that all necessary files are correctly
 * identified and their order is honored during import.
 *
 * @author rkosegi
 */
public class CollectImportFilesTest {

    private static final Logger LOG = LoggerFactory.getLogger(CollectImportFilesTest.class);

    private Path daeximDir;
    private Path tempDir;

    private static final String[] FILE_NAMES = {
        Util.FILE_PREFIX + LogicalDatastoreType.OPERATIONAL.name().toLowerCase() + ".json",
        Util.FILE_PREFIX + LogicalDatastoreType.OPERATIONAL.name().toLowerCase() + "_opendaylight-inventory.json",
        Util.FILE_PREFIX + LogicalDatastoreType.OPERATIONAL.name().toLowerCase()
                + "_opendaylight-inventory@2013-08-19.json" };

    @Before
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("daexim-test-tmp");
        daeximDir = Files.createDirectory(tempDir.resolve(Util.DAEXIM_DIR));
        LOG.info("Created temp directory : {}", daeximDir);
        System.setProperty("karaf.home", tempDir.toString());
        Files.createFile(daeximDir.resolve(FILE_NAMES[0]));
        Files.createFile(daeximDir.resolve(FILE_NAMES[1]));
        Files.createFile(daeximDir.resolve(FILE_NAMES[2]));
    }

    @After
    public void tearDown() throws IOException {
        Files.delete(daeximDir.resolve(FILE_NAMES[0]));
        Files.delete(daeximDir.resolve(FILE_NAMES[1]));
        Files.delete(daeximDir.resolve(FILE_NAMES[2]));
        Files.delete(daeximDir);
    }

    @Test
    public void test() throws IOException {
        final ImmediateImportInput input = new ImmediateImportInputBuilder().setCheckModels(true).build();
        final DOMDataBroker domDataBroker = mock(DOMDataBroker.class);
        final SchemaService schemaService = mock(SchemaService.class);
        final ImportTask rt = new ImportTask(input, domDataBroker, schemaService, false, mock(Callback.class));
        final ListMultimap<LogicalDatastoreType, File> df = rt.dataFiles;
        assertTrue(df.get(LogicalDatastoreType.CONFIGURATION).isEmpty());
        assertThat(df.get(LogicalDatastoreType.OPERATIONAL).get(2).toString(), new EndsWith("@2013-08-19.json"));
        assertThat(df.get(LogicalDatastoreType.OPERATIONAL).get(1).toString(),
                new EndsWith("opendaylight-inventory.json"));
        assertThat(df.get(LogicalDatastoreType.OPERATIONAL).get(0).toString(),
                new EndsWith(LogicalDatastoreType.OPERATIONAL.name().toLowerCase() + ".json"));
    }
}
