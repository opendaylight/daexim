/*
 * Copyright (c) 2024 PANTHEON.tech, s.r.o. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import org.opendaylight.daexim.DataImportBootReady;
import org.osgi.service.component.annotations.Component;

/**
 * OSGi implementation of {@link DataImportBootReady} contract.
 */
@Component(immediate = true, service = OSGiDataImportBootReady.class, factory = OSGiDataImportBootReady.FACTORY_NAME)
final class OSGiDataImportBootReady implements DataImportBootReady {
    static final String FACTORY_NAME = "org.opendaylight.daexim.impl.OSGiDataImportReady";

}
