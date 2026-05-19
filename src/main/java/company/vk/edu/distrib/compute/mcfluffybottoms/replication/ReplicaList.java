package company.vk.edu.distrib.compute.mcfluffybottoms.replication;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.mcfluffybottoms.FileDao;

public class ReplicaList {
    private final List<Node> nodes;
    private final Set<String> ids = ConcurrentHashMap.newKeySet();
    private static final Logger log = LoggerFactory.getLogger(ReplicaList.class);

    class Node {
        public final Dao<byte[]> dao;
        AtomicBoolean isEnabled = new AtomicBoolean(false);

        Node(int i) throws IllegalArgumentException, IOException {
            this.isEnabled.set(true);
            this.dao = new FileDao(Path.of("node-" + i));
        }

        public void disable() {
            this.isEnabled.set(false);
        }

        public void enable() {
            this.isEnabled.set(true);
        }

        public boolean enabled() {
            return this.isEnabled.get();
        }
    }

    class RequestResult {
        public final int code;
        public final byte[] value;
        public final int responded;

        public RequestResult(int code, byte[] value, int responded) {
            this.code = code;
            this.value = value == null ? null : value.clone();
            this.responded = responded;
        }

        public void sendRequest(HttpExchange exchange) throws IOException {
            exchange.sendResponseHeaders(this.code, this.value.length);
            exchange.getResponseBody().write(this.value);
        }
    }

    public ReplicaList(int factor) throws IllegalArgumentException, IOException {
        this.nodes = initNodes(factor);
    }

    private List<Node> initNodes(int factor) throws IllegalArgumentException, IOException {
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < factor; ++i) {
            nodes.add(new Node(i));
        }
        return nodes;
    }

    public int numberOfReplicas() {
        return nodes.size();
    }

    public void disableReplica(int nodeId) {
        Node node = tryGet(nodeId);
        node.disable();
    }

    public void enableReplica(int nodeId) throws IllegalArgumentException, IOException {
        Node node = tryGet(nodeId);
        syncNodes(node);
        node.enable();
    }

    private Node tryGet(int nodeId) {
        Node node = nodes.get(nodeId);
        if (node == null) {
            throw new NoSuchElementException("nodeId '" + nodeId + "' not found.");
        }
        return node;
    }

    private List<Node> getNodeOrderById(String id) {
        int start = Math.abs(id.hashCode()) % nodes.size();
        List<Node> finalOrder = new ArrayList<>();

        for (int i = 0; i < nodes.size(); i++) {
            finalOrder.add(nodes.get((start + i) % nodes.size()));
        }
        return finalOrder;
    }

    private void syncNodes(Node toSync) throws IllegalArgumentException, IOException {
        for (String id : ids) {
            syncNode(toSync, id);
        }
    }

    private void syncNode(Node toSync, String id) throws IllegalArgumentException, IOException {
        byte[] val = null;
        for (Node node : getNodeOrderById(id)) {
            if (!node.enabled() || node == toSync) {
                continue;
            }

            try {
                val = node.dao.get(id);
                if (val != null) {
                    break;
                }
            } catch (NoSuchElementException e) {
                toSync.dao.delete(id);
            }
        }

        if (val != null) {
            toSync.dao.upsert(id, val);
        } else {
            toSync.dao.delete(id);
        }
    }

    public RequestResult handleGet(String id, int ack) throws IOException {
        int responded = 0;
        byte[] val = null;

        for (Node node : getNodeOrderById(id)) {
            if (!node.enabled()) {
                continue;
            }

            try {
                byte[] v = node.dao.get(id);
                if (v != null) {
                    val = v;
                }
                responded++;
            } catch (NoSuchElementException e) {
                responded++;
            }
        }

        if (responded < ack) {
            return new RequestResult(500, new byte[0], responded);
        }

        if (val == null) {
            return new RequestResult(404, new byte[0], responded);
        } else {
            return new RequestResult(200, val, responded);
        }
    }

    public RequestResult handlePut(byte[] body, String id, int ack) throws IOException {
        int responded = 0;

        for (Node node : getNodeOrderById(id)) {
            if (!node.enabled()) {
                continue;
            }

            try {
                node.dao.upsert(id, body);
                responded++;
            } catch (NoSuchElementException e) {
                log.warn("Failed to upsert id {} on node {}", id, e);
            }
        }

        ids.add(id);
        if (responded < ack) {
            return new RequestResult(500, new byte[0], responded);
        }

        return new RequestResult(201, new byte[0], responded);
    }

    public RequestResult handleDelete(String id, int ack) throws IOException {
        int responded = 0;

        for (Node node : getNodeOrderById(id)) {
            if (!node.enabled()) {
                continue;
            }

            try {
                node.dao.delete(id);
                responded++;
            } catch (NoSuchElementException e) {
                log.warn("Failed to delete id {} on node {}", id, e);
            }
        }

        if (responded < ack) {
            return new RequestResult(500, new byte[0], responded);
        }

        return new RequestResult(202, new byte[0], responded);
    }
}
