package de.jth.simpleyarnapp;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerResourceIncreasePBImpl;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.*;
import java.util.*;

public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
    private final static long SLEEP_INTERVAL = 100; // Check every 100ms for completed containers
    private final static long TIMEOUT = 60000; // Wait at most 40 seconds for a container to finish
    private final NMClientAsync nmClientAsync;
    private final int maxContainers;
    private int runningContainers;
    private final YarnConfiguration configuration;
    private final ApplicationAttemptId applicationAttemptId;
    private final AMRMClientAsync amRMClient;
    private NMCallbackHandler containerListener;
    private final static int MAX_ITERATIONS = Short.MAX_VALUE * 100;
    private final static int MIN_ITERATIONS = Short.MAX_VALUE * 60;
    private final static String command =
            "/home/jth/work/MA/repos/hadoop/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/simple-yarn-app/src/main/c/build/pi";

    public ApplicationMaster(int runningContainers) {
        configuration = new YarnConfiguration();
        maxContainers = runningContainers;
        this.runningContainers = runningContainers;

        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();

        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        amRMClient.init(configuration);
        amRMClient.start();

        containerListener = createNMCallbackHandler();

        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(configuration);
        nmClientAsync.start();

        applicationAttemptId = getApplicationAttemptID();
    }

    /**
     * Manages callbacks from NodeManager.
     * @return
     */
    NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler(this);
    }

    /**
     * Get the application attempt id for the interaction with the RM. Is obtained from the AM Container ID.
     */
    private ApplicationAttemptId getApplicationAttemptID() {
        final Map<String, String> envs = System.getenv();
        final String containerIdString = envs.get(ApplicationConstants.Environment.CONTAINER_ID.toString());

        if (containerIdString == null) {
            throw new IllegalArgumentException("ContainerId not set in the environment");
        }

        ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
        return containerId.getApplicationAttemptId();
    }

    public static void main(String[] args) throws Exception {
        final int numberContainers = Integer.parseInt(args[0]);
        System.out.println("JTH: Starting with " + numberContainers + " containers");
        clearProgressFiles(numberContainers);
        ApplicationMaster am = new ApplicationMaster(numberContainers);
        am.runMainLoop();
    }

    private static void clearProgressFiles(int numberContainers) {
        System.out.println("JTH: Clearing progress files");
        for (int i = 1; i < numberContainers + 1; ++i) {
            try {
                final PrintWriter writer = new PrintWriter("/tmp/" + i);
                writer.print("");
                writer.close();
            } catch (Exception e) {
            }
        }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("JTH: Completed container " + status.getContainerId());
            synchronized (this) {
                runningContainers--;
            }
        }
    }

    private long getRandomIterations() {
        Random rand = new Random();

        int randomNum = rand.nextInt((MAX_ITERATIONS - MIN_ITERATIONS) + 1) + MIN_ITERATIONS;

        return randomNum;
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        System.out.println("JTH: onContainersAllocated()");
        for (Container container : containers) {
            // Launch container by create ContainerLaunchContext
            ContainerLaunchContext ctx =
                    Records.newRecord(ContainerLaunchContext.class);
            final String complete_cmd = command + " " + getRandomIterations() + " /tmp/" + container.getId().getContainerId() +
                            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
            System.out.println("JTH: Executing cmd: " + complete_cmd);
            System.out.println("JTH: New ContainerID: " + container.getId().getContainerId());
            ctx.setCommands(Collections.singletonList(complete_cmd));
            System.out.println("JTH: Launching container " + container.getId());
            // This actually starts the container with the given context
            try {
                nmClientAsync.startContainerAsync(container, ctx);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    @Override
    public void onShutdownRequest() {
        System.out.println("JTH: onShutdownRequest()");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        System.out.println("JTH: onNodesUpdated()");
    }

    @Override
    public float getProgress() {
        final Map<String, String> envs = System.getenv();

        final String containerIdString =
                envs.get(ApplicationConstants.Environment.CONTAINER_ID.toString());
        if (containerIdString == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException("ContainerId not set in the environment");
        }

        // set progress to deliver to RM on next heartbeat

       // ContainerId containerId = ConverterUtils.toContainerId(containerIdString);

        ContainerResourceIncreaseRequest foo = new ContainerResourceIncreaseRequest() {
            @Override
            public ContainerId getContainerId() {
                return null;
            }

            @Override
            public void setContainerId(ContainerId containerId) {

            }

            @Override
            public Resource getCapability() {
                return null;
            }

            @Override
            public void setCapability(Resource capability) {

            }
        };

/**
        try {
            final FileInputStream tmp = new FileInputStream("/tmp/" + i);
            final String progressStr = IOUtils.toString(tmp).trim();
            if (progressStr.isEmpty() || Float.parseFloat(progressStr) > 99) {
                continue;
            }
            final float progress = Float.parseFloat(progressStr);
            if (progress > 50 && progress < 52) {
                System.out.println("Container " + containerId.getContainerId() + " reached progress > 50%");
            }
            System.out.println("Container " + containerId.getContainerId() + ": " + progressStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
*/
        float progress = (float) (maxContainers - runningContainers) / this.maxContainers;
        return progress;
    }

    @Override
    public void onError(Throwable e) {
        System.out.println("JTH: Something went wrong: " + e.getMessage());
        // Possible to get the causing container here?
    }

    public boolean doneWithContainers() {
        return runningContainers == 0;
    }

    public void runMainLoop() throws Exception {
        long currentTime = 0;
        AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(configuration);
        rmClient.start();

        // Register with ResourceManager
        System.out.println("JTH: registerApplicationMaster");
        rmClient.registerApplicationMaster("", 0, "");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(64);
        capability.setVirtualCores(1);

        // Make container requests to ResourceManager
        for (int i = 0; i < runningContainers; ++i) {
            AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
            System.out.println("[AM] Making res-req " + i);
            rmClient.addContainerRequest(containerAsk);
        }


        System.out.println("[AM] waiting for containers to finish");
        while (!doneWithContainers()) {
            Thread.sleep(SLEEP_INTERVAL);
            currentTime += SLEEP_INTERVAL;
            if (currentTime >= TIMEOUT) {
                System.out.println("JTH: Timeout reached, killing running container");
                break;
            }
        }

        // TODO: Get more information about specific containers
        System.out.println("[AM] unregisterApplicationMaster 0");
        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
    }
}
