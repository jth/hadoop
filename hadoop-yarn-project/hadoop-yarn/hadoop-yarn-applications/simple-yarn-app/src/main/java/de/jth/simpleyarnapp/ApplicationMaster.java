package de.jth.simpleyarnapp;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
    private final static long SLEEP_INTERVAL = 100; // Check every 100ms for completed containers
    private final static long TIMEOUT = 20000; // Wait at most 20 seconds for a container to finish
    private final NMClient nmClient;
    private int numberContainers;
    private final YarnConfiguration configuration;
    private final static int MIN_ITERATIONS = Short.MAX_VALUE;
    private final static int MAX_ITERATIONS = Integer.MAX_VALUE;
    private final static String command =
            "/home/jth/work/MA/repos/hadoop/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/simple-yarn-app/src/main/c/build/pi";

    public ApplicationMaster(int numberContainers) {
        configuration = new YarnConfiguration();
        this.numberContainers = numberContainers;
        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();
    }

    public static void main(String[] args) throws Exception {
        final int numberContainers = Integer.parseInt(args[0]);
        System.out.println("JTH: Starting with " + numberContainers + " containers");
        ApplicationMaster am = new ApplicationMaster(Integer.parseInt(args[0]));
        am.runMainLoop();
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("JTH: Completed container " + status.getContainerId());
            synchronized (this) {
                numberContainers--;
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
        for (Container container : containers) {
            // Launch container by create ContainerLaunchContext
            ContainerLaunchContext ctx =
                    Records.newRecord(ContainerLaunchContext.class);
            final String complete_cmd = command + " " + getRandomIterations() +
                            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
            System.out.println("JTH: Executing cmd: " + complete_cmd);
            ctx.setCommands(Collections.singletonList(complete_cmd));
            System.out.println("JTH: Launching container " + container.getId());

            // This actually starts the container with the given context
            try {
                nmClient.startContainer(container, ctx);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    @Override
    public void onShutdownRequest() {

    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {

    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable e) {
        System.out.println("JTH: Something went wrong: " + e.getMessage());
        // Possible to get the causing container here?
    }

    public boolean doneWithContainers() {
        return numberContainers == 0;
    }

    public void runMainLoop() throws Exception {
        long currentTime = 0;
        AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(configuration);
        rmClient.start();

        // Register with ResourceManager
        System.out.println("JTH: registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("JTH: registerApplicationMaster 1");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(64);
        capability.setVirtualCores(1);

        // Make container requests to ResourceManager
        for (int i = 0; i < numberContainers; ++i) {
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
