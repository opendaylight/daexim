/*
 * Copyright (c) 2024 PANTHEON.tech, s.r.o. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import org.opendaylight.daexim.DataImportBootReady;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi implementation of {@link DataImportBootReady} contract.
 */
@Component(factory = OSGiDataImportBootReady.FACTORY_NAME)
public final class OSGiDataImportBootReady implements DataImportBootReady {
    private static final Logger LOG = LoggerFactory.getLogger(OSGiDataImportBootReady.class);

    static final String FACTORY_NAME = "org.opendaylight.daexim.impl.OSGiDataImportReady";

    @Activate
    public OSGiDataImportBootReady() {
        LOG.info("Published OSGi service {}", DataImportBootReady.class);
    }

    @Deactivate
    void deactivate() {
        LOG.debug("Published OSGi service {}", DataImportBootReady.class);
    }
}
