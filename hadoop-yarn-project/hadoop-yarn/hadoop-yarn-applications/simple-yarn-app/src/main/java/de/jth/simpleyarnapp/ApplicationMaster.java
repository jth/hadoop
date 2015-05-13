package de.jth.simpleyarnapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.util.Collections;

public class ApplicationMaster {

    final static String command =
            "/home/jth/work/MA/repos/hadoop/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/simple-yarn-app/src/main/c/build/pi";

    public static void main(String[] args) throws Exception {

        final String iters = args[0];
        final int numberContainers = Integer.valueOf(args[1]);

        // Initialize clients to ResourceManager and NodeManagers
        // There is no specific configuration here, but possible.
        Configuration conf = new YarnConfiguration();

        // ApplicationMaster <-> ResourceManager, this is the request for containers.
        AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        // ApplicationMaster <-> NodeManager, this launches containers (?)
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        System.out.println("registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("registerApplicationMaster 1");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(64);
        capability.setVirtualCores(1);

        // Make container requests to ResourceManager
        // Use the previously created rmClient for this.
        for (int i = 0; i < numberContainers; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            System.out.println("Making res-req " + i);
            rmClient.addContainerRequest(containerAsk);
        }


        // Obtain allocated containers, launch and check for responses
        // This is basically busy-waiting
        int responseId = 0;

        // Is it possible to get more than one allocated container per request?
        AllocateResponse response = rmClient.allocate(responseId++);
        for (Container container : response.getAllocatedContainers()) {
            // Launch container by create ContainerLaunchContext
            ContainerLaunchContext ctx =
                    Records.newRecord(ContainerLaunchContext.class);
            System.out.println("JTH: Executing cmd: " + command);
            ctx.setCommands(
                    // Redirects the output of the command to stdout or stderr, depending on the output
                    Collections.singletonList(
                            command + " " + iters +
                                    " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                    " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                    ));
            System.out.println("Launching container " + container.getId());
            // This actually starts the container with the given context
            nmClient.startContainer(container, ctx);
        }
        // Container has been "completed", increment the counter to exit while-loop eventually
        for (ContainerStatus status : response.getCompletedContainersStatuses()) {
            System.out.println("Completed container " + status.getContainerId());
        }

        // Un-register with ResourceManager, this is basically cleanup.
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
    }
}
