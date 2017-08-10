/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UtilTest {

    private static final Logger LOG = LoggerFactory.getLogger(UtilTest.class);

    private static final String ALTERNATIVE_DIR = "SOME_DIR";
    private static final String ANOTHER_DIR = "ANOTHER_DIR";
    private static final String CFG_FILE = Util.CFG_FILE_NAME;

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
        Files.deleteIfExists(tempDir.resolve(ANOTHER_DIR));
        Files.deleteIfExists(tempDir.resolve(ALTERNATIVE_DIR));
        Files.delete(etcDir);
        Files.delete(tempDir);
    }

    private void setPropertyFileContent(String content) throws IOException {
        com.google.common.io.Files.write(content, etcDir.resolve(CFG_FILE).toFile(), UTF_8);
    }

    @Test
    public void testGetBackupDir() throws IOException {
        String path;
        LOG.info("Scenario #1 - property file does not exist");
        path = Util.getDaeximDir(false);
        LOG.info("Directory : {}", path);
        assertEquals(daeximDir.toFile().getAbsolutePath(), path);

        LOG.info("Scenario #2 - property file exists, but property is not set");
        setPropertyFileContent("");
        path = Util.getDaeximDir(false);
        LOG.info("Directory : {}", path);
        assertEquals(daeximDir.toFile().getAbsolutePath(), path);

        LOG.info("Scenario #3 - property file exists, property is set");
        setPropertyFileContent(Util.DAEXIM_DIR_PROP + "=" + tempDir.toString() + File.separatorChar + ALTERNATIVE_DIR);
        path = Util.getDaeximDir(false);
        LOG.info("Directory : {}", path);
        assertEquals(tempDir.toString() + File.separatorChar + ALTERNATIVE_DIR, path);

        LOG.info("Scenario #4 - property file exists, property is set but using property substitution (interpolation)");
        setPropertyFileContent(Util.DAEXIM_DIR_PROP + "=${karaf.home}/" + ANOTHER_DIR);
        path = Util.getDaeximDir(false);
        LOG.info("Directory : {}", path);
        assertEquals(tempDir.toString() + File.separatorChar + ANOTHER_DIR, path);
    }
}
