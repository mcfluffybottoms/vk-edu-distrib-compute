package company.vk.edu.distrib.compute.mcfluffybottoms.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.mcfluffybottoms.clustering.hashers.ConsistentHasher;
import company.vk.edu.distrib.compute.mcfluffybottoms.clustering.hashers.Hasher;
import company.vk.edu.distrib.compute.mcfluffybottoms.clustering.hashers.RendezvousHasher;
import company.vk.edu.distrib.compute.mcfluffybottoms.InMemoryDao;

public class McfluffybottomsKVCluster implements KVCluster {

    private static String endpointPrefix = "http://localhost:";
    private final Map<String, KVService> endpoints;
    private final List<String> endpointsKeys;

    public enum HasherType {
        CONSISTENT,
        RENDEZVOUS
    }

    public McfluffybottomsKVCluster(List<Integer> ports, HasherType hasherType) {
        this.endpoints = new ConcurrentHashMap<>();
        this.endpointsKeys = new ArrayList<>();

        for (int port : ports) {
            endpointsKeys.add(endpointPrefix + port);
        }

        Hasher hasher;
        hasher = switch (hasherType) {
            case HasherType.CONSISTENT -> new ConsistentHasher(endpointsKeys);
            case HasherType.RENDEZVOUS -> new RendezvousHasher(endpointsKeys);
        };

        for (int port : ports) {
            endpoints.put(endpointPrefix + port, createNode(port, hasher));
        }
    }

    private KVService createNode(int port, Hasher hasher) {
        return new McfluffybottomsKVServiceProxy(port, new InMemoryDao(), hasher, endpointPrefix + port);
    }

    @Override
    public void start() {
        for (String endpoint : endpointsKeys) {
            start(endpoint);
        }
    }

    @Override
    public void start(String endpoint) {
        KVService node = endpoints.get(endpoint);

        if (node == null) {
            throw new IllegalArgumentException("Endpoint " + endpoint + " does not exist.");
        }

        node.start();
    }

    @Override
    public void stop() {
        for (String endpoint : endpoints.keySet()) {
            stop(endpoint);
        }
    }

    @Override
    public void stop(String endpoint) {
        KVService node = endpoints.get(endpoint);

        if (node == null) {
            throw new IllegalArgumentException("Endpoint " + endpoint + " does not exist.");
        }

        node.stop();
    }

    @Override
    public List<String> getEndpoints() {
        return endpointsKeys;
    }
}
