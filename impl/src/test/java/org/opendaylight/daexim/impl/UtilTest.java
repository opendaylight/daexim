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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opendaylight.daexim.impl.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;

public class UtilTest {
    private static final String ALTERNATIVE_DIR = "SOME_DIR";
    private static final Logger LOG = LoggerFactory.getLogger(UtilTest.class);
    private static final String CFG_FILE = "daexim.cfg";
    private Path tempDir;
    private Path daeximDir;
    private Path etcDir;

    @Before
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("daexim-test-tmp");
        etcDir = Files.createDirectory(tempDir.resolve("etc"));
        daeximDir = Files.createDirectory(tempDir.resolve(Util.DAEXIM_DIR));
        System.setProperty("karaf.home", tempDir.toString());
        System.setProperty("karaf.etc", etcDir.toString());
    }

    @After
    public void tearDown() throws IOException {
        Files.delete(daeximDir);
        Files.deleteIfExists(etcDir.resolve(CFG_FILE));
        Files.deleteIfExists(tempDir.resolve(ALTERNATIVE_DIR));
        Files.delete(etcDir);
        Files.delete(tempDir);
    }

    private void setPropertyFileContent(String content) throws IOException {
        try (final ByteArrayInputStream is = new ByteArrayInputStream(content.getBytes(Charsets.UTF_8))) {
            ByteStreams.copy(is, new FileOutputStream(etcDir.resolve(CFG_FILE).toFile()));
        }
    }

    @Test
    public void testGetBackupDir() throws IOException {
        String path;
        LOG.info("Scenario #1 - property file does not exists");
        path = Util.getDaeximDir();
        LOG.info("Directory : {}", path);
        assertEquals(daeximDir.toFile().getAbsolutePath(), path);

        LOG.info("Scenario #2 - property file exists, but property is not set");
        setPropertyFileContent("");
        path = Util.getDaeximDir();
        LOG.info("Directory : {}", path);
        assertEquals(daeximDir.toFile().getAbsolutePath(), path);

        LOG.info("Scenario #3 - property file exists, property is set");
        setPropertyFileContent(Util.DAEXIM_DIR_PROP + "=" + tempDir.toString() + File.separatorChar + ALTERNATIVE_DIR);
        path = Util.getDaeximDir();
        LOG.info("Directory : {}", path);
        assertEquals(tempDir.toString() + File.separatorChar + ALTERNATIVE_DIR, path);
    }
}
