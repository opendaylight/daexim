/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Address;
import org.apache.pekko.cluster.Cluster;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.opendaylight.controller.cluster.ActorSystemProvider;
import org.opendaylight.daexim.spi.NodeNameProvider;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.RequireServiceComponentRuntime;

/**
 * Implementation of {@link NodeNameProvider} which uses Akka cluster API to get
 * local cluster member name from ShardManager.
 *
 * @author rkosegi
 */
@Singleton
@Component(immediate = true)
@RequireServiceComponentRuntime
public final class AkkaNodeNameProvider implements NodeNameProvider {
    private final ActorSystem actorSystem;

    @Inject
    @Activate
    public AkkaNodeNameProvider(@Reference final ActorSystemProvider provider) {
        actorSystem = provider.getActorSystem();
    }

    @Override
    public String getNodeName() {
        final Address selfAddress = Cluster.get(actorSystem).selfAddress();
        return selfAddress.host().get() + ':' + selfAddress.port().get();
    }
}
