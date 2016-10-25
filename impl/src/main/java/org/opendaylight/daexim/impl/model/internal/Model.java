/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl.model.internal;

import com.google.gson.annotations.SerializedName;

/**
 * This class represents internal storage structure for Yang model serialized as
 * JSON object
 *
 * @author rkosegi
 *
 */
public class Model {
    private String module;
    private String namespace;
    @SerializedName("revision-date")
    private String revision;

    public String getRevision() {
        return revision;
    }

    public void setRevision(String revision) {
        this.revision = revision;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    @Override
    public String toString() {
        return "Module [module=" + module + ", namespace=" + namespace + ", revision=" + revision + "]";
    }
}
