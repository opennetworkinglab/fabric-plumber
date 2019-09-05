/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onosproject.plumber;

import com.google.common.collect.Streams;
import org.onlab.util.SharedExecutors;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.PortNumber;
import org.onosproject.net.flow.DefaultFlowRule;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.criteria.PiCriterion;
import org.onosproject.net.host.HostEvent;
import org.onosproject.net.host.HostListener;
import org.onosproject.net.host.HostService;
import org.onosproject.net.pi.model.PiActionId;
import org.onosproject.net.pi.model.PiActionParamId;
import org.onosproject.net.pi.model.PiMatchFieldId;
import org.onosproject.net.pi.model.PiTableId;
import org.onosproject.net.pi.runtime.PiAction;
import org.onosproject.net.pi.runtime.PiActionParam;
import org.onosproject.net.topology.PathService;
import org.onosproject.net.topology.TopologyEvent;
import org.onosproject.net.topology.TopologyListener;
import org.onosproject.net.topology.TopologyService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Simple fabric host mesh connectivity plumber.
 */
@Component(immediate = true)
public class FabricPlumber {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected TopologyService topologyService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected HostService hostService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected PathService pathService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected FlowRuleService flowRuleService;

    private static final int PRIORITY = 4321;
    private static final long MAC_MASK = 0xffffffffffffL;

    private ExecutorService executor = SharedExecutors.getSingleThreadExecutor();
    private TopologyListener topoListener = new InternalTopologyListener();
    private HostListener hostListener = new InternalHostListener();
    private ApplicationId appId;

    @Activate
    protected void activate() {
        appId = coreService.registerApplication("org.onosproject.fabric.plumber");
        topologyService.addListener(topoListener);
        hostService.addListener(hostListener);
        executor.submit(FabricPlumber.this::processMesh);
        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        topologyService.removeListener(topoListener);
        hostService.addListener(hostListener);
        flowRuleService.removeFlowRulesById(appId);
        log.info("Stopped");
    }

    private void processMesh() {
        flowRuleService.removeFlowRulesById(appId);
        hostService.getHosts().forEach(this::plumbToDest);
    }

    private void plumbToDest(Host dst) {
        Streams.stream(hostService.getHosts())
                .filter(src -> !src.id().equals(dst.id()))
                .forEach(src -> plumpSrcToDest(src, dst));
    }

    private void plumpSrcToDest(Host src, Host dst) {
        // Compute path from host to host
        Set<Path> paths = pathService.getPaths(src.id(), dst.id());
        if (paths.isEmpty()) {
            return;
        }

        Path path = paths.iterator().next();
        log.info("Plumbing path from {} to {} via {}", src.id(), dst.id(), path);
        plumbPath(path, dst);
    }

    private void plumbPath(Path path, Host dst) {
        Link[] links = new Link[path.links().size()];
        path.links().toArray(links);

        for (int i = 1; i < links.length; i++) {
            plumbFlowRules(links[i].src().deviceId(),
                           links[i - 1].dst().port(), links[i].src().port(),
                           dst);
        }
    }

    private void plumbFlowRules(DeviceId deviceId,
                                PortNumber inPort, PortNumber outPort,
                                Host dst) {
        FlowRule f1 = filterRule(deviceId, inPort);
        FlowRule f2 = bridgingRule(deviceId, dst, outPort);
        FlowRule f3 = forwardRule(deviceId, outPort);
        FlowRule f4 = egressRule(deviceId, outPort);
        flowRuleService.applyFlowRules(f1, f2, f3, f4);
    }

    private FlowRule filterRule(DeviceId deviceId, PortNumber inPort) {
        PiCriterion match = PiCriterion.builder()
                .matchExact(PiMatchFieldId.of("ig_port"), inPort.toLong())
                .matchExact(PiMatchFieldId.of("vlan_is_valid"), 0)
                .build();

        PiAction action = PiAction.builder()
                .withId(PiActionId.of("FabricIngress.filtering.permit_with_internal_vlan"))
                .withParameter(new PiActionParam(PiActionParamId.of("vlan_id"), (short) 100))
                .build();

        return DefaultFlowRule.builder()
                .forDevice(deviceId).fromApp(appId).makePermanent().withPriority(PRIORITY)
                .forTable(PiTableId.of("FabricIngress.filtering.ingress_port_vlan"))
                .withSelector(DefaultTrafficSelector.builder().matchPi(match).build())
                .withTreatment(DefaultTrafficTreatment.builder().piTableAction(action).build())
                .build();
    }

    private FlowRule bridgingRule(DeviceId deviceId, Host dst, PortNumber outPort) {
        PiCriterion match = PiCriterion.builder()
                .matchExact(PiMatchFieldId.of("vlan_id"), 100)
                .matchTernary(PiMatchFieldId.of("eth_dst"), dst.mac().toLong(), MAC_MASK)
                .build();

        PiAction action = PiAction.builder()
                .withId(PiActionId.of("FabricIngress.forwarding.set_next_id_bridging"))
                .withParameter(new PiActionParam(PiActionParamId.of("next_id"), outPort.toLong()))
                .build();

        return DefaultFlowRule.builder()
                .forDevice(deviceId).fromApp(appId).makePermanent().withPriority(PRIORITY)
                .forTable(PiTableId.of("FabricIngress.forwarding.bridging"))
                .withSelector(DefaultTrafficSelector.builder().matchPi(match).build())
                .withTreatment(DefaultTrafficTreatment.builder().piTableAction(action).build())
                .build();
    }

    private FlowRule forwardRule(DeviceId deviceId, PortNumber outPort) {
        PiCriterion match = PiCriterion.builder()
                .matchExact(PiMatchFieldId.of("next_id"), outPort.toLong())
                .build();

        PiAction action = PiAction.builder()
                .withId(PiActionId.of("FabricIngress.next.output_simple"))
                .withParameter(new PiActionParam(PiActionParamId.of("port_num"), outPort.toLong()))
                .build();

        return DefaultFlowRule.builder()
                .forDevice(deviceId).fromApp(appId).makePermanent().withPriority(PRIORITY)
                .forTable(PiTableId.of("FabricIngress.next.simple"))
                .withSelector(DefaultTrafficSelector.builder().matchPi(match).build())
                .withTreatment(DefaultTrafficTreatment.builder().piTableAction(action).build())
                .build();
    }

    private FlowRule egressRule(DeviceId deviceId, PortNumber outPort) {
        PiCriterion match = PiCriterion.builder()
                .matchExact(PiMatchFieldId.of("vlan_id"), 100)
                .matchExact(PiMatchFieldId.of("eg_port"), outPort.toLong())
                .build();

        PiAction action = PiAction.builder()
                .withId(PiActionId.of("FabricEgress.egress_next.pop_vlan"))
                .build();

        return DefaultFlowRule.builder()
                .forDevice(deviceId).fromApp(appId).makePermanent().withPriority(PRIORITY)
                .forTable(PiTableId.of("FabricEgress.egress_next.egress_vlan"))
                .withSelector(DefaultTrafficSelector.builder().matchPi(match).build())
                .withTreatment(DefaultTrafficTreatment.builder().piTableAction(action).build())
                .build();
    }

    private class InternalTopologyListener implements TopologyListener {
        @Override
        public void event(TopologyEvent event) {
            executor.submit(FabricPlumber.this::processMesh);
        }
    }

    private class InternalHostListener implements HostListener {
        @Override
        public void event(HostEvent event) {
            executor.submit(FabricPlumber.this::processMesh);
        }
    }
}
