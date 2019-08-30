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
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.PortNumber;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.host.HostEvent;
import org.onosproject.net.host.HostListener;
import org.onosproject.net.host.HostService;
import org.onosproject.net.topology.PathService;
import org.onosproject.net.topology.TopologyEvent;
import org.onosproject.net.topology.TopologyListener;
import org.onosproject.net.topology.TopologyService;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Executable;
import java.util.Dictionary;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.onlab.util.Tools.get;

/**
 * Simple fabric host mesh connectivity plumber.
 */
@Component(immediate = true,
        service = {SomeInterface.class},
        property = {
                "someProperty=Some Default String Value",
        })
public class AppComponent implements SomeInterface {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Some configurable property.
     */
    private String someProperty;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected ComponentConfigService cfgService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected TopologyService topologyService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected HostService hostService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected PathService pathService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected FlowRuleService flowRuleService;

    private ExecutorService executor = SharedExecutors.getSingleThreadExecutor();
    private TopologyListener topoListener = new InternalTopologyListener();
    private HostListener hostListener = new InternalHostListener();


    @Activate
    protected void activate() {
        cfgService.registerProperties(getClass());
        topologyService.addListener(topoListener);
        hostService.addListener(hostListener);
        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        cfgService.unregisterProperties(getClass(), false);
        topologyService.removeListener(topoListener);
        hostService.addListener(hostListener);

        log.info("Stopped");
    }

    @Modified
    public void modified(ComponentContext context) {
        Dictionary<?, ?> properties = context != null ? context.getProperties() : new Properties();
        if (context != null) {
            someProperty = get(properties, "someProperty");
        }
        log.info("Reconfigured");
    }

    private void processMesh() {
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
        plumbPath(path, src, dst);
    }

    private void plumbPath(Path path, Host src, Host dst) {
        Link links[] = new Link[path.links().size()];
        path.links().toArray(links);

        for (int i = 1; i < links.length; i++) {
            plumbFlowRules(links[i].src().deviceId(),
                           links[i-1].dst().port(), links[i].src().port(),
                           src, dst);
        }
    }

    private void plumbFlowRules(DeviceId deviceId,
                                PortNumber inPort, PortNumber outPort,
                                Host src, Host dst) {

    }

    @Override
    public void someMethod() {
        log.info("Invoked");
    }

    private class InternalTopologyListener implements TopologyListener {
        @Override
        public void event(TopologyEvent event) {
            executor.submit(AppComponent.this::processMesh);
        }

    }

    private class InternalHostListener implements HostListener {
        @Override
        public void event(HostEvent event) {
            executor.submit(AppComponent.this::processMesh);
        }
    }
}
