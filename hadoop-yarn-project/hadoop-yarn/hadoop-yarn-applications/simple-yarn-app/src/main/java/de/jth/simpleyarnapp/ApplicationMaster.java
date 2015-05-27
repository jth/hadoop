package de.jth.simpleyarnapp;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
    private final static long SLEEP_INTERVAL = 100; // Check every 100ms for completed containers
    private final static long TIMEOUT = 60000; // Wait at most 40 seconds for a container to finish
    private final NMClient nmClient;
    private int numberContainers;
    private final YarnConfiguration configuration;
    private final static int MAX_ITERATIONS = Short.MAX_VALUE * 100;
    private final static int MIN_ITERATIONS = Short.MAX_VALUE * 60;
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
                nmClient.startContainer(container, ctx);
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
        for (int i = 1; i < numberContainers + 1; ++i) {
            try {
                final FileInputStream tmp = new FileInputStream("/tmp/" + i);
                final String progressStr = IOUtils.toString(tmp).trim();
                if (progressStr.isEmpty() || Float.parseFloat(progressStr) > 99) {
                    continue;
                }
                final float progress = Float.parseFloat(progressStr);
                if (progress > 50 && progress < 52) {
                    System.out.println("Container " + i + " reached progress > 50%");
                }
                System.out.println("Container " + i + ": " + progressStr);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
