package company.vk.edu.distrib.compute.mcfluffybottoms.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.mcfluffybottoms.replication.ReplicaList.RequestResult;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVServiceImpl implements ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);

    private final HttpServer server;
    private final int serverPort;
    private final ReplicaList replicas;

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";

    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";

    public KVServiceImpl(int port) throws IOException {
        this.server = initServer(port);
        this.serverPort = port;
        int factor = ReplicationConfigUtils.loadReplicationFactor();
        this.replicas = new ReplicaList(factor);
    }

    private HttpServer initServer(int port) {
        try {
            HttpServer s;
            s = HttpServer.create(new InetSocketAddress(port), 0);
            s.createContext(PATH_STATUS, this::handleStatus);
            s.createContext(PATH_ENTITY, this::handleEntity);
            return s;
        } catch (IOException e) {
            log.error("Failed to create HTTP server on port {}.", port, e);
            throw new IllegalStateException("Server initialization failed", e);
        }
    }

    @Override
    public void start() {
        server.start();
        log.info("Started");
    }

    @Override
    public void stop() {
        server.stop(0);
        log.info("Stopping");
    }

    @Override
    public int port() {
        return this.serverPort;
    }

    @Override
    public int numberOfReplicas() {
        return replicas.numberOfReplicas();
    }

    @Override
    public void disableReplica(int nodeId) {
        replicas.disableReplica(nodeId);
        log.info("Replica on nodeId {} disabled.", nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        try {
            replicas.enableReplica(nodeId);
        } catch (IOException e) {
            log.error("Could not sync replica with nodeId {}.", nodeId, e);
            return;
        }
        log.info("Replica on nodeId {} enabled.", nodeId);
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> q = new ConcurrentHashMap<>();
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf('=');
            q.put(pair.substring(0, idx), pair.substring(idx + 1));
        }
        return q;
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (GET.equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, 0);
            } else {
                exchange.sendResponseHeaders(405, 0);
            }
            log.debug("Status received.");
        }
    }

    private Integer getAck(Map<String, String> args) throws IOException {
        boolean hasAsk = args.containsKey("ack") && args.get("ack") != null;
        if (!hasAsk) {
            log.info("Ack size set to default 1.");
        }
        int ack = hasAsk ? Integer.parseInt(args.get("ack")) : 1;
        if (ack > replicas.numberOfReplicas() || ack < 0) {
            log.error("Bad ack size {}", ack);
            return null;
        }

        return ack;
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            String query = exchange.getRequestURI().getQuery();
            if (query == null) {
                log.error("Query is empty.");
                exchange.sendResponseHeaders(400, 0);
                return;
            }
            Map<String, String> args = parseQuery(query);

            String id = args.get("id");
            if (id == null || id.isBlank()) {
                log.error("No id present in the query.");
                exchange.sendResponseHeaders(400, 0);
                return;
            }

            Integer ack = getAck(args);
            if (ack == null) {
                exchange.sendResponseHeaders(400, 0);
                return;
            }

            executeMethod(exchange, id, ack);
            log.debug("Entity handled.");
        }
    }

    private void executeMethod(HttpExchange exchange, String id, int ack) throws IOException {
        String method = exchange.getRequestMethod();
        switch (method) {
            case GET ->
                handleGet(exchange, id, ack);
            case PUT ->
                handlePut(exchange, id, ack);
            case DELETE ->
                handleDelete(exchange, id, ack);
            default ->
                exchange.sendResponseHeaders(405, 0);
        }
    }

    private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
        log.info("Get from id {}, need {} replicas to respond.", id, ack);

        RequestResult result = replicas.handleGet(id, ack);

        switch (result.code) {
            case 500 -> log.warn("Get from id {} failed, responded {}, ack {}", id, result.responded, ack);
            case 404 -> log.error("No element on id {}.", id);
            case 200 -> log.debug("Got element on id {}.", id);
            default -> log.warn("Unexpected status code {} for id {}", result.code, id);
        }

        log.debug("Data got on id {}.", id);
        result.sendRequest(exchange);
    }

    private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
        log.info("Put from id {}, need {} replicas to respond.", id, ack);
        byte[] body = exchange.getRequestBody().readAllBytes();

        RequestResult result = replicas.handlePut(body, id, ack);

        switch (result.code) {
            case 500 -> log.warn("Put in id {} failed, responded {}, ack {}", id, result.responded, ack);
            case 201 -> log.debug("Got element on id {}.", id);
            default -> log.warn("Unexpected status code {} for id {}", result.code, id);
        }

        log.debug("Data inserted on id {}.", id);
        result.sendRequest(exchange);
    }

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        log.info("Delete from id {}, need {} replicas to respond.", id, ack);
        RequestResult result = replicas.handleDelete(id, ack);

        switch (result.code) {
            case 500 -> log.warn("Delete in id {} failed, responded {}, ack {}", id, result.responded, ack);
            case 202 -> log.debug("{} deleted.", id);
            default -> log.warn("Unexpected status code {} for id {}", result.code, id);
        }

        log.debug("Data inserted on id {}.", id);
        result.sendRequest(exchange);
    }
}
