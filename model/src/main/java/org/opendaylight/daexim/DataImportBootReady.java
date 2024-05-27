/*
 * Copyright (c) 2017 Red Hat, Inc. and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim;

/**
 * Functionality ready marker OSGi service indicating that the (asynchronous) "import-on-boot"
 * has successfully fully completed (or, alternatively, that there was nothing to import, on boot).
 *
 * @author Michael Vorburger.ch
 */
public interface DataImportBootReady {
    // Just a marker interface
}
