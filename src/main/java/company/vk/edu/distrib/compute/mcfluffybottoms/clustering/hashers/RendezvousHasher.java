package company.vk.edu.distrib.compute.mcfluffybottoms.clustering.hashers;

import java.util.List;
import java.util.Objects;

public class RendezvousHasher implements Hasher {
    private final List<String> nodes;

    public RendezvousHasher(List<String> nodes) {
        this.nodes = nodes;
    }

    @Override
    public String getHash(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Cannot get hash from null value.");
        }
        if (nodes.isEmpty()) {
            throw new IllegalStateException("No nodes. Nothing to hash from.");
        }

        String bestNode = null;
        Integer maxHash = Integer.MIN_VALUE;

        for (String node : nodes) {
            int hash = getPairHash(key, node);

            if (hash > maxHash) {
                bestNode = node;
                maxHash = hash;
            }
        }

        return bestNode;
    }

    private int getPairHash(String left, String right) {
        return Objects.hash(left, right);
    }
}
