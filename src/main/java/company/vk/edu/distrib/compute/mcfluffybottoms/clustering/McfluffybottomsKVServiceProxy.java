package company.vk.edu.distrib.compute.mcfluffybottoms.clustering;

import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.mcfluffybottoms.clustering.hashers.Hasher;

public class McfluffybottomsKVServiceProxy implements KVService {
    private static final Logger log = LoggerFactory.getLogger(McfluffybottomsKVServiceProxy.class);

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";

    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";

    private final Duration timeout = Duration.ofSeconds(1);

    private final String url;
    private final int port;
    private HttpServer server;
    private HttpClient client;
    private final Dao<byte[]> dao;

    private final Hasher hasher;

    private final AtomicBoolean running = new AtomicBoolean(false);

    public McfluffybottomsKVServiceProxy(int port, Dao<byte[]> dao, Hasher hasher, String url) {
        this.dao = dao;
        this.hasher = hasher;
        this.url = url;
        this.port = port;
    }

    private HttpServer initServer(int port) {
        try {
            HttpServer s;
            s = HttpServer.create(new InetSocketAddress(port), 0);

            s.createContext(PATH_STATUS, this::handleStatus);
            s.createContext(PATH_ENTITY, this::handleEntity);
            return s;
        } catch (Exception e) {
            log.error("Failed to create HTTP server on port {}.", port, e);
            throw new IllegalStateException("Server initialization failed", e);
        }
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

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
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

            String targetURL = this.hasher.getHash(id);

            if (targetURL.equals(url)) {
                log.debug("Handling exchange on {} locally.", targetURL);
                switch (method) {
                    case GET ->
                        handleGet(exchange, id);
                    case PUT ->
                        handlePut(exchange, id);
                    case DELETE ->
                        handleDelete(exchange, id);
                    default ->
                        exchange.sendResponseHeaders(405, 0);
                }
            } else {
                log.debug("Proxy request to node with url {} from {}.", targetURL, url);
                proxy(exchange, targetURL, id);
            }

            log.debug("Entity handled.");
        }
    }

    private void proxy(HttpExchange exchange, String target, String id) throws IOException {
        try {
            URI uri = URI.create(target + PATH_ENTITY + "?id=" + id);

            HttpRequest.Builder requestBuild = HttpRequest.newBuilder().uri(uri).timeout(timeout);

            String method = exchange.getRequestMethod();
            switch (method) {
                case "GET" ->
                    requestBuild.GET();
                case "PUT" ->
                    requestBuild.PUT(HttpRequest.BodyPublishers.ofByteArray(exchange.getRequestBody().readAllBytes()));
                case "DELETE" ->
                    requestBuild.DELETE();
                default ->
                    exchange.sendResponseHeaders(405, 0);
            }

            HttpResponse<byte[]> response = client.send(requestBuild.build(), HttpResponse.BodyHandlers.ofByteArray());

            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            exchange.getResponseBody().write(response.body());
        } catch (Exception e) {
            String message = "Server error: " + e.getMessage();
            log.error(message);
            exchange.sendResponseHeaders(500, message.length());
            exchange.getResponseBody().write(message.getBytes());
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] val = dao.get(id);
            exchange.sendResponseHeaders(200, val.length);
            exchange.getResponseBody().write(val);
            log.debug("Got element on id {}.", id);
        } catch (NoSuchElementException e) {
            log.error("No element on id {}.", id);
            exchange.sendResponseHeaders(404, 0);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] data = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, data);
        log.debug("Data inserted on id {}.", id);
        exchange.sendResponseHeaders(201, 0);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        log.debug("{} deleted.", id);
        exchange.sendResponseHeaders(202, 0);
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

    @Override
    public void start() {
        if (running.get()) {
            log.info("Already started on port {}.", port);
        } else {
            this.server = initServer(port);
            this.client = HttpClient.newHttpClient();
            server.start();
            running.set(true);
            log.info("Starting on port {}.", port);
        }
    }

    @Override
    public void stop() {
        if (running.get()) {
            server.stop(0);
            client.close();
            running.set(false);
            log.info("Stopping on port {}.", port);
        } else {
            log.info("Already stopped on port {}.", port);
        }
    }
}
