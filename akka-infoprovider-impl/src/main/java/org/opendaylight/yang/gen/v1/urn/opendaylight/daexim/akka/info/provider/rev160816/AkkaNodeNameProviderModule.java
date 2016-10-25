/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.akka.info.provider.rev160816;

import org.opendaylight.daexim.impl.AkkaNodeNameProvider;

public class AkkaNodeNameProviderModule extends org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.akka.info.provider.rev160816.AbstractAkkaNodeNameProviderModule {
    public AkkaNodeNameProviderModule(org.opendaylight.controller.config.api.ModuleIdentifier identifier, org.opendaylight.controller.config.api.DependencyResolver dependencyResolver) {
        super(identifier, dependencyResolver);
    }

    public AkkaNodeNameProviderModule(org.opendaylight.controller.config.api.ModuleIdentifier identifier, org.opendaylight.controller.config.api.DependencyResolver dependencyResolver, org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.akka.info.provider.rev160816.AkkaNodeNameProviderModule oldModule, java.lang.AutoCloseable oldInstance) {
        super(identifier, dependencyResolver, oldModule, oldInstance);
    }

    @Override
    public void customValidation() {
        // add custom validation form module attributes here.
    }

    @Override
    public java.lang.AutoCloseable createInstance() {
        final AkkaNodeNameProvider provider = new AkkaNodeNameProvider();
        provider.setActorSystem(getActorSystemProviderServiceDependency().getActorSystem());
        return provider;
    }
}
