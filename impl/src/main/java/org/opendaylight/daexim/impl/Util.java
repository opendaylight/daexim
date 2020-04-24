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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ListMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import org.opendaylight.daexim.impl.model.internal.Model;
import org.opendaylight.mdsal.common.api.LogicalDatastoreType;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev130715.DateAndTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Util {

    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    private static final TimeZone TZ_UTC = TimeZone.getTimeZone("UTC");
    private static final String[] DATE_AND_TIME_FORMATS = {
        "yyyy-MM-dd'T'HH:mm:ss'Z'",
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" };
    public static final String FILE_PREFIX = "odl_backup_";
    public static final String FILE_SUFFIX = ".json";
    public static final String CFG_FILE_NAME = "org.opendaylight.daexim.cfg";
    public static final String ETC_CFG_FILE = "${karaf.etc}/" + CFG_FILE_NAME;
    public static final String DAEXIM_DIR_PROP = "daexim.dir";
    public static final String INTERNAL_LOCAL_NAME = "daexim";
    public static final String DAEXIM_DIR = INTERNAL_LOCAL_NAME;
    public static final String DAEXIM_BOOT_SUBDIR = "boot";
    private static final String KARAF_HOME = "karaf.home";
    public static final String DEFAULT_DIR_LOCATION = "${karaf.home}/" + DAEXIM_DIR;

    private static final BiMap<LogicalDatastoreType, String> STORE_NAME_MAPPINGS = ImmutableBiMap
            .of(LogicalDatastoreType.CONFIGURATION, "config", LogicalDatastoreType.OPERATIONAL, "operational");
    public static final String INTERNAL_MODULE_NAME = "data-export-import-internal";

    private Util() {
        // utility class constructor
    }

    public static String storeNameByType(LogicalDatastoreType type) {
        return STORE_NAME_MAPPINGS.get(type);
    }

    public static LogicalDatastoreType storeTypeFromName(String name) {
        return STORE_NAME_MAPPINGS.inverse().get(name);
    }

    public static Path getDaeximFilePath(boolean isBooting, LogicalDatastoreType type) {
        return Paths.get(getDaeximDir(isBooting), FILE_PREFIX + storeNameByType(type).toLowerCase() + FILE_SUFFIX);
    }

    public static Path getModelsFilePath(boolean isBooting) {
        return Paths.get(getDaeximDir(isBooting), FILE_PREFIX + "models.json");
    }

    private static String interpolateProp(final String source, final String propName, final String defValue) {
        return source.replace("${" + propName + "}", System.getProperty(propName, defValue));
    }

    private static String getDaeximDirInternal() {
        final String propFile = interpolateProp(ETC_CFG_FILE, "karaf.etc", "." + File.separatorChar + "etc");
        final Properties props = new Properties();
        try (InputStream is = new FileInputStream(propFile)) {
            props.load(is);
            if (props.containsKey(DAEXIM_DIR_PROP)) {
                String propVal = props.getProperty(DAEXIM_DIR_PROP);
                // NB: java.util.Properties does NOT perform any property substitution (interpolation)
                return interpolateProp(propVal, KARAF_HOME, "." + File.separatorChar + DAEXIM_DIR);
            } else {
                // This is caught immediately below (only; NOT propagated)
                throw new IOException("Property '" + DAEXIM_DIR_PROP + "' was not found");
            }
        // FileNotFoundException is expected in tests when the src/main/config/daexim.cfg is not installed
        } catch (FileNotFoundException e) {
            LOG.warn("Configuration not found, so just using fixed ${karaf.home}/daexim/");
            return interpolateProp(DEFAULT_DIR_LOCATION, KARAF_HOME, "." + File.separatorChar + DAEXIM_DIR);
        } catch (IOException e) {
            LOG.error("Failed to load existing property file (ignoring, and using fixed ${karaf.home}/daexim/): {}",
                    propFile, e);
            return interpolateProp(DEFAULT_DIR_LOCATION, KARAF_HOME, "." + File.separatorChar + DAEXIM_DIR);
        }
    }

    @VisibleForTesting
    static String getDaeximDir(boolean isBooting) {
        final Path daeximDir = isBooting
                ? Paths.get(getDaeximDirInternal(), DAEXIM_BOOT_SUBDIR)
                : Paths.get(getDaeximDirInternal());
        try {
            Files.createDirectories(daeximDir);
            return daeximDir.toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to get location of daexim directory", e);
        }
    }

    public static List<Model> parseModels(final InputStream is) {
        final Gson g = new GsonBuilder().create();
        final InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        return g.fromJson(reader, new TypeToken<List<Model>>() {
        }.getType());
    }

    /**
     * Attempts to parse given date string using patterns described
     * https://tools.ietf.org/html/rfc6991#page-11 with exception that ONLY UTC
     * patterns are accepted.
     *
     * @param dateStr
     *            date string to parse
     * @return {@link Date}
     * @throws IllegalArgumentException
     *             if none patterns matched given input
     */
    public static Date parseDate(final String dateStr) {
        for (final String fmt : DATE_AND_TIME_FORMATS) {
            try {
                // constructing SimpleDateFormat instances can be costly,
                // but this utility method is rarely used (and is private to
                // application).
                final SimpleDateFormat sdf = new SimpleDateFormat(fmt);
                sdf.setTimeZone(TZ_UTC);
                return sdf.parse(dateStr);
            } catch (ParseException e) {
                // ignore
            }
        }
        throw new IllegalArgumentException(
                "Unrecognized DateAndTime value : " + dateStr + " (only UTC date is accepted)");
    }

    /**
     * Transform given {@link Date} into {@link DateAndTime} using pattern
     * yyyy-MM-dd'T'HH:mm:ss'Z'.
     *
     * @param date
     *            date to format
     * @return {@link DateAndTime}
     */
    public static DateAndTime toDateAndTime(Date date) {
        return new DateAndTime(dateToUtcString(date));
    }

    /**
     * Transform given {@link Date} into {@link String} using pattern
     * yyyy-MM-dd'T'HH:mm:ss'Z'.
     *
     * @param date
     *            date to format
     * @return String
     */
    public static String dateToUtcString(Date date) {
        final SimpleDateFormat sdf = new SimpleDateFormat(DATE_AND_TIME_FORMATS[0]);
        sdf.setTimeZone(TZ_UTC);
        return sdf.format(date);
    }

    /**
     * Collects all data files in dump directory.
     */
    public static ListMultimap<LogicalDatastoreType, File> collectDataFiles(boolean isBooting) {
        final Path daeximDir = Paths.get(Util.getDaeximDir(isBooting));
        final ListMultimap<LogicalDatastoreType, File> dataFiles = ArrayListMultimap.create();
        for (final LogicalDatastoreType dst : LogicalDatastoreType.values()) {
            // collect all json files related to given datastore
            dataFiles.putAll(dst, collectDatastoreFiles(daeximDir, dst));
            // sort them to honor order during import
            final List<File> unsorted = dataFiles.get(dst);
            Collections.sort(unsorted, (f1, f2) -> f1.getAbsolutePath().length() - f2.getAbsolutePath().length());
        }
        return dataFiles;
    }

    private static List<File> collectDatastoreFiles(final Path daeximDir, final LogicalDatastoreType dst) {
        final File[] arr = daeximDir.toFile().listFiles((FilenameFilter) (dir, name)
            -> name.startsWith(Util.FILE_PREFIX + Util.storeNameByType(dst).toLowerCase())
            && name.endsWith(Util.FILE_SUFFIX));
        return Arrays.asList(arr != null ? arr : new File[] {});
    }

    public static boolean isModelFilePresent(boolean isBooting) {
        return getModelsFilePath(isBooting).toFile().exists();
    }
}
