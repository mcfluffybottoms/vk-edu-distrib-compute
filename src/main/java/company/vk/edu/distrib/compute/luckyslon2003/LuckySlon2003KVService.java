package company.vk.edu.distrib.compute.luckyslon2003;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.Executors;

public class LuckySlon2003KVService implements KVService {

    private static final Logger log = LoggerFactory.getLogger(LuckySlon2003KVService.class);

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";

    private final HttpServer server;
    private final Dao<byte[]> dao;

    public LuckySlon2003KVService(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(Executors.newCachedThreadPool());
        this.server.createContext(PATH_STATUS, this::handleStatus);
        this.server.createContext(PATH_ENTITY, this::handleEntity);
    }

    @Override
    public void start() {
        server.start();
        if (log.isInfoEnabled()) {
            log.info("LuckySlon2003KVService started on port {}", server.getAddress().getPort());
        }
    }

    @Override
    public void stop() {
        server.stop(1);
        try {
            dao.close();
        } catch (IOException e) {
            log.error("Error closing DAO", e);
        }
        log.info("LuckySlon2003KVService stopped");
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!Objects.equals(exchange.getRequestMethod(), "GET")) {
            sendEmpty(exchange, 405);
            return;
        }
        sendEmpty(exchange, 200);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String id = queryParam(exchange.getRequestURI(), "id");
        if (id == null || id.isEmpty()) {
            sendEmpty(exchange, 400);
            return;
        }

        switch (exchange.getRequestMethod().toUpperCase(java.util.Locale.ROOT)) {
            case "GET" -> handleGet(exchange, id);
            case "PUT" -> handlePut(exchange, id);
            case "DELETE" -> handleDelete(exchange, id);
            default -> sendEmpty(exchange, 405);
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] data = dao.get(id);
            exchange.sendResponseHeaders(200, data.length);
            try (var out = exchange.getResponseBody()) {
                out.write(data);
            }
        } catch (NoSuchElementException e) {
            sendEmpty(exchange, 404);
        } catch (IllegalArgumentException e) {
            sendEmpty(exchange, 400);
        } catch (IOException e) {
            log.error("GET error for id={}", id, e);
            sendEmpty(exchange, 500);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        try (InputStream body = exchange.getRequestBody()) {
            byte[] data = body.readAllBytes();
            dao.upsert(id, data);
            sendEmpty(exchange, 201);
        } catch (IllegalArgumentException e) {
            sendEmpty(exchange, 400);
        } catch (IOException e) {
            log.error("PUT error for id={}", id, e);
            sendEmpty(exchange, 500);
        }
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        try {
            dao.delete(id);
            sendEmpty(exchange, 202);
        } catch (IllegalArgumentException e) {
            sendEmpty(exchange, 400);
        } catch (IOException e) {
            log.error("DELETE error for id={}", id, e);
            sendEmpty(exchange, 500);
        }
    }

    private static void sendEmpty(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
        exchange.close();
    }

    private static String queryParam(URI uri, String name) {
        String query = uri.getRawQuery();
        if (query == null) {
            return null;
        }
        String prefix = name + "=";
        for (String pair : query.split("&")) {
            if (pair.startsWith(prefix)) {
                return pair.substring(prefix.length());
            }
        }
        return null;
    }
}
