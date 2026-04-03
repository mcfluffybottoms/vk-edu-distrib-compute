package company.vk.edu.distrib.compute.mcfluffybottoms;

import java.io.IOException;

import com.sun.net.httpserver.HttpServer;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.Dao;

import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;

public class KVServiceImpl implements KVService {

    //private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    private final HttpServer server;
    private final Dao<byte[]> dao;

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";

    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.server = initServer(port);
    }

    private HttpServer initServer(int port) {
        try {
            HttpServer s;
            s = HttpServer.create(new InetSocketAddress(port), 0);
            s.createContext(PATH_STATUS, this::handleStatus);
            s.createContext(PATH_ENTITY, this::handleEntity);
            return s;
        } catch (IOException e) {
            //log.error("Failed to create HTTP server on port {}.", port, e);
            throw new RuntimeException("Server initialization failed", e);
        }
    }

    @Override
    public void start() {
        server.start();
        //log.info("Started");
    }

    @Override
    public void stop() {
        server.stop(0);
        //log.info("Stopping");
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (GET.equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, 0);
            } else {
                exchange.sendResponseHeaders(405, 0);
            }
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String query = exchange.getRequestURI().getQuery();
            if (query == null) {
                exchange.sendResponseHeaders(400, 0);
                return;
            }

            ConcurrentHashMap<String, String> args = parseQuery(query);
            String id = args.get("id");
            if (id == null || id.isBlank()) {
                exchange.sendResponseHeaders(400, 0);
                return;
            }

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
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] val = dao.get(id);
            exchange.sendResponseHeaders(200, val.length);
            exchange.getResponseBody().write(val);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, 0);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] data = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, data);
        exchange.sendResponseHeaders(201, 0);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(202, 0);
    }

    private ConcurrentHashMap<String, String> parseQuery(String query) {
        ConcurrentHashMap<String, String> q = new ConcurrentHashMap<>();
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf('=');
            q.put(pair.substring(0, idx), pair.substring(idx + 1));
        }
        return q;
    }

}
