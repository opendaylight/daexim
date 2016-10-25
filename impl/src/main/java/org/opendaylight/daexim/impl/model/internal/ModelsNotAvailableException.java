/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl.model.internal;

public class ModelsNotAvailableException extends Exception {
    private static final long serialVersionUID = -2056800159703827029L;

    public ModelsNotAvailableException() {
    }

    public ModelsNotAvailableException(String message) {
        super(message);
    }
}
