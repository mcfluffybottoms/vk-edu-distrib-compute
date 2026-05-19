package company.vk.edu.distrib.compute.mcfluffybottoms.replication;

import java.util.Properties;

public final class ReplicationConfigUtils {

    private static final int DEFAULT_FACTOR = 3;
    private static final String CONFIG_PATH = "replication.properties";

    private ReplicationConfigUtils() {
    }

    public static int loadReplicationFactor() {
        Properties props = new Properties();
        try {
            props.load(ReplicationConfigUtils.class.getClassLoader().getResourceAsStream(CONFIG_PATH));
            return Integer.parseInt(props.getProperty("replication.factor", String.valueOf(DEFAULT_FACTOR)));
        } catch (Exception ex) {
            return DEFAULT_FACTOR;
        }
    }
}
