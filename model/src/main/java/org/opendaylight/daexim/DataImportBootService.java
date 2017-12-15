/*
 * Copyright (c) 2017 Red Hat, Inc. and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim;

import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.DataExportImportService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.rev160921.OperationStatus;

/**
 * Service API for the "auto-import-on-boot" feature.
 * @author Michael Vorburger.ch
 */
public interface DataImportBootService {

    /**
     * Check last import status, of this node only. Does not return status about
     * restore operation on per-node basis; see
     * {@link DataExportImportService#statusImport()} for that. This is a very
     * light weight operation (i.e. no data store access &amp; no RPC overhead), suitable to be
     * invoked very frequently (contrary to
     * {@link DataExportImportService#statusImport()}).
     */
    OperationStatus statusImportOnLocalNode();

    /**
     * Await {@link #statusImportOnLocalNode()} OK.
     * <ul><li>If there were no files to "auto-import-on-boot",
     * or if there were but meanwhile they have been processed, then this returns (fast).
     * <li>If there is a "auto-import-on-boot" in progress,
     * then this awaits its completion (blocking); progress will be warn logged periodically.
     * <li>If there was a "auto-import-on-boot" but that failed,
     * then this throws an IllegalStateException.
     * </ul>
     * @param blockingWhat name of Class &amp; method being blocked (just for logging)
     *
     * @deprecated Using {@link DataImportBootReady} is, usually, a better alternative to this
     */
    @Deprecated
    void awaitBootImport(String blockingWhat) throws IllegalStateException;

}
