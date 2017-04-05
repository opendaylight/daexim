/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import org.opendaylight.daexim.spi.NodeNameProvider;

/**
 * Implementation of {@link NodeNameProvider} which uses Akka cluster API to get
 * local cluster member name from ShardManager.
 *
 * @author rkosegi
 */
public class AkkaNodeNameProvider implements NodeNameProvider, AutoCloseable {
    private ActorSystem actorSystem;

    @Override
    public void close() {
        // noop
    }

    @Override
    public String getNodeName() {
        final Cluster cluster = Cluster.get(actorSystem);
        return cluster.selfAddress().host().get() + ':' + cluster.selfAddress().port().get();
    }

    public void setActorSystem(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }
}
