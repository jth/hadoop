import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Created by jth on 5/8/15.
 */
public class ApplicationMaster {
    private final YarnConfiguration conf;

    public ApplicationMaster() {
        conf = new YarnConfiguration();
    }

    public static void main(String args[]) {
        final ApplicationMaster appMaster = new ApplicationMaster();
    }

}
