/*
 * Copyright (C) 2016 AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2016 Brocade Communications Systems, Inc. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.config.rev160803;

import org.opendaylight.daexim.impl.DataExportImportAppProvider;

public class DataExportImportConfigModule extends org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.config.rev160803.AbstractDataExportImportConfigModule {
    public DataExportImportConfigModule(org.opendaylight.controller.config.api.ModuleIdentifier identifier, org.opendaylight.controller.config.api.DependencyResolver dependencyResolver) {
        super(identifier, dependencyResolver);
    }

    public DataExportImportConfigModule(org.opendaylight.controller.config.api.ModuleIdentifier identifier, org.opendaylight.controller.config.api.DependencyResolver dependencyResolver, org.opendaylight.yang.gen.v1.urn.opendaylight.daexim.config.rev160803.DataExportImportConfigModule oldModule, java.lang.AutoCloseable oldInstance) {
        super(identifier, dependencyResolver, oldModule, oldInstance);
    }

    @Override
    public void customValidation() {
        // add custom validation form module attributes here.
    }

    @Override
    public java.lang.AutoCloseable createInstance() {
        final DataExportImportAppProvider provider = new DataExportImportAppProvider();
        provider.setDomDataBroker(getDomDataBrokerDependency());
        provider.setRpcProviderRegistry(getRpcRegistryDependency());
        provider.setSchemaService(getSchemaServiceDependency());
        provider.setNodeNameProvider(getNodeNameProviderDependency());
        provider.setDataBroker(getDataBrokerDependency());
        provider.init();
        return provider;
    }

}
