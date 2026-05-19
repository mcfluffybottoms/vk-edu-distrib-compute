package company.vk.edu.distrib.compute.mcfluffybottoms.clustering.hashers;

import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class ConsistentHasher implements Hasher {
    private final NavigableMap<Integer, String> ring;

    public ConsistentHasher(List<String> nodes) {
        this.ring = new TreeMap<>();
        for (String node : nodes) {
            ring.put(getNodeHash(node), node);
        }
    }

    @Override
    public String getHash(String key) {
        Integer hash = getNodeHash(key);

        NavigableMap<Integer, String> tailMap = ring.tailMap(hash, true);

        if (tailMap.isEmpty()) {
            return ring.firstEntry().getValue();
        } else {
            return tailMap.firstEntry().getValue();
        }
    }

    private int getNodeHash(String value) {
        return Objects.hash(value);
    }
}
