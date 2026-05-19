package company.vk.edu.distrib.compute.wolfram158;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVServiceImpl implements KVService {
    private final HttpServer server;

    public KVServiceImpl(final int port, final Dao<byte[]> dao) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        addStatusHandler();
        addEntityHandler(dao);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    private void addStatusHandler() {
        server.createContext("/v0/status", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, -1);
            } catch (IOException e) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_UNAVAILABLE, -1);
            }
        });
    }

    private void addEntityHandler(final Dao<byte[]> dao) {
        server.createContext("/v0/entity", exchange -> {
            switch (exchange.getRequestMethod()) {
                case HttpMethodConstants.GET: {
                    handleGetEntity(exchange, dao);
                    break;
                }

                case HttpMethodConstants.PUT: {
                    handlePutEntity(exchange, dao);
                    break;
                }

                case HttpMethodConstants.DELETE: {
                    handleDeleteEntity(exchange, dao);
                    break;
                }

                default: {
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
                    exchange.close();
                    break;
                }
            }
        });
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    private void handleGetEntity(
            final HttpExchange exchange,
            final Dao<byte[]> dao
    ) throws IOException {
        try {
            final Map<String, List<String>> queries = Utils.extractQueryParams(exchange.getRequestURI().getQuery());
            final List<String> values = queries.get(QueryParamConstants.ID);
            if (values.isEmpty() || values.getFirst().isEmpty()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }
            final byte[] response = dao.get(values.getFirst());
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
        } finally {
            exchange.close();
        }
    }

    private void handlePutEntity(
            final HttpExchange exchange,
            final Dao<byte[]> dao
    ) throws IOException {
        try (exchange; InputStream is = exchange.getRequestBody()) {
            final Map<String, List<String>> queries = Utils.extractQueryParams(exchange.getRequestURI().getQuery());
            final List<String> values = queries.get(QueryParamConstants.ID);
            if (values.isEmpty() || values.getFirst().isEmpty()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }
            dao.upsert(values.getFirst(), is.readAllBytes());
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_CREATED, -1);
        }
    }

    private void handleDeleteEntity(
            final HttpExchange exchange,
            final Dao<byte[]> dao
    ) throws IOException {
        try (exchange) {
            final Map<String, List<String>> queries = Utils.extractQueryParams(exchange.getRequestURI().getQuery());
            final List<String> values = queries.get(QueryParamConstants.ID);
            if (values.isEmpty() || values.getFirst().isEmpty()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
            } else {
                dao.delete(values.getFirst());
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_ACCEPTED, -1);
            }
        }
    }
}
