/*
 * Copyright (c) 2017 Red Hat, Inc. and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.daexim.impl;

import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.TopologyId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.TopologyKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

/**
 * Constants for src/test/resources/odl_backup_operational.json.
 */
public interface TestBackupData {

    TopologyId TOPOLOGY_ID = new TopologyId("topo-id");
    TopologyId TOPOLOGY_ID_2 = new TopologyId("topo-id-2");

    InstanceIdentifier<Topology> TOPOLOGY_II = InstanceIdentifier
            .create(NetworkTopology.class)
                .child(Topology.class,
                        new TopologyKey(TestBackupData.TOPOLOGY_ID));

}
