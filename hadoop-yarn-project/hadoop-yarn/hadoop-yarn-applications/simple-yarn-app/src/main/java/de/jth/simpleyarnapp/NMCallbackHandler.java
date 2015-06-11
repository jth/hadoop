package de.jth.simpleyarnapp;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by jth on 6/1/15.
 */
public class NMCallbackHandler implements NMClientAsync.CallbackHandler {

    private ConcurrentMap<ContainerId, Container> containers =
            new ConcurrentHashMap<ContainerId, Container>();
    private final ApplicationMaster applicationMaster;

    public NMCallbackHandler(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    public void addContainer(ContainerId containerId, Container container) {
        containers.putIfAbsent(containerId, container);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        System.out.println("Succeeded to stop Container " + containerId);
        containers.remove(containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
                                          ContainerStatus containerStatus) {
        System.out.println("Container Status: id=" + containerId + ", status=" + containerStatus);
    }

    @Override
    public void onContainerStarted(ContainerId containerId,
                                   Map<String, ByteBuffer> allServiceResponse) {
        System.out.println("Succeeded to start Container " + containerId);
        Container container = containers.get(containerId);
        if (container != null) {
            applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
        }
        if (applicationMaster.timelineClient != null) {
            ApplicationMaster.publishContainerStartEvent(
                    applicationMaster.timelineClient, container,
                    applicationMaster.domainId, applicationMaster.appSubmitterUgi);
        }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        System.out.println("Failed to start Container " + containerId);
        containers.remove(containerId);
        applicationMaster.numCompletedContainers.incrementAndGet();
        applicationMaster.numFailedContainers.incrementAndGet();
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
        System.out.println("Failed to query the status of Container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        System.out.println("Failed to stop Container " + containerId);
        containers.remove(containerId);
    }
}
}
