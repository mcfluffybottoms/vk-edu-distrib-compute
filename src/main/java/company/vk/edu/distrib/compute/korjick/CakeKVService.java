package company.vk.edu.distrib.compute.korjick;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class CakeKVService implements KVService {

    private static final int BACKLOG_VALUE = 0;
    private static final int STOP_SERVER_DELAY = 1;
    private static final int EMPTY_BODY_LENGTH = -1;
    private static final String QUERY_PARAM_SEPARATOR = "&";
    private static final String QUERY_VALUE_SEPARATOR = "=";
    private static final String ID_QUERY_PARAM_VALUE = "id";
    private static final String EMPTY_QUERY_PARAM_VALUE = "";

    private static final String HTTP_METHOD_GET = "GET";
    private static final String HTTP_METHOD_PUT = "PUT";
    private static final String HTTP_METHOD_DELETE = "DELETE";

    private static final int HTTP_STATUS_OK = 200;
    private static final int HTTP_STATUS_CREATED = 201;
    private static final int HTTP_STATUS_ACCEPTED = 202;
    private static final int HTTP_STATUS_BAD_REQUEST = 400;
    private static final int HTTP_STATUS_NOT_FOUND = 404;
    private static final int HTTP_STATUS_METHOD_NOT_ALLOWED = 405;
    private static final int HTTP_STATUS_INTERNAL_ERROR = 500;

    private static final Logger log = LoggerFactory.getLogger(CakeKVService.class);

    private final HttpServer server;
    private final Dao<byte[]> dao;

    public CakeKVService(int port, Dao<byte[]> dao) throws IOException {
        var inetSocketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
        this.server = HttpServer.create(inetSocketAddress, BACKLOG_VALUE);
        this.dao = dao;
        initServer();
    }

    @Override
    public void start() {
        this.server.start();
        log.info("HTTP server started");
    }

    @Override
    public void stop() {
        log.info("Stopping HTTP server");
        this.server.stop(STOP_SERVER_DELAY);
        log.info("HTTP server stopped");
    }

    private void initServer() {
        this.server.createContext("/v0/status", ErrorHandler.fromDelegate(this::status));
        this.server.createContext("/v0/entity", ErrorHandler.fromDelegate(this::entity));
    }

    private void entity(HttpExchange httpExchange) throws IOException {
        final var method = httpExchange.getRequestMethod();
        final var id = parseIdQueryParam(httpExchange);
        if (id == null) {
            throw new IllegalArgumentException("query param id is required");
        }

        log.info("Received {} request for entity {}", method, id);
        switch (method) {
            case HTTP_METHOD_GET -> {
                try {
                    final var value = this.dao.get(id);
                    httpExchange.sendResponseHeaders(HTTP_STATUS_OK, value.length);
                    httpExchange.getResponseBody().write(value);
                } catch (NoSuchElementException e) {
                    httpExchange.sendResponseHeaders(HTTP_STATUS_NOT_FOUND, EMPTY_BODY_LENGTH);
                }
            }
            case HTTP_METHOD_PUT -> {
                final var body = httpExchange.getRequestBody().readAllBytes();
                this.dao.upsert(id, body);
                httpExchange.sendResponseHeaders(HTTP_STATUS_CREATED, EMPTY_BODY_LENGTH);
            }
            case HTTP_METHOD_DELETE -> {
                this.dao.delete(id);
                httpExchange.sendResponseHeaders(HTTP_STATUS_ACCEPTED, EMPTY_BODY_LENGTH);
            }
            case null, default -> sendNotAllowedResponse(httpExchange);
        }
    }

    private void status(HttpExchange httpExchange) throws IOException {
        final var method = httpExchange.getRequestMethod();
        switch (method) {
            case HTTP_METHOD_GET -> httpExchange.sendResponseHeaders(HTTP_STATUS_OK, EMPTY_BODY_LENGTH);
            case null, default -> sendNotAllowedResponse(httpExchange);
        }
    }

    private void sendNotAllowedResponse(HttpExchange httpExchange) throws IOException {
        httpExchange.sendResponseHeaders(HTTP_STATUS_METHOD_NOT_ALLOWED, EMPTY_BODY_LENGTH);
    }

    private String parseIdQueryParam(HttpExchange httpExchange) {
        final var query = httpExchange.getRequestURI().getQuery();
        final var params = query.split(QUERY_PARAM_SEPARATOR);
        for (var param : params) {
            final var splitParamTuple = param.split(QUERY_VALUE_SEPARATOR);
            if (splitParamTuple.length == 2 && ID_QUERY_PARAM_VALUE.equals(splitParamTuple[0])) {
                return splitParamTuple[1];
            } else if (splitParamTuple.length == 1 && ID_QUERY_PARAM_VALUE.equals(splitParamTuple[0])) {
                return EMPTY_QUERY_PARAM_VALUE;
            }
        }

        return null;
    }

    public static final class ErrorHandler implements HttpHandler {

        private final HttpHandler delegate;

        private ErrorHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                try {
                    this.delegate.handle(exchange);
                } catch (IllegalArgumentException e) {
                    exchange.sendResponseHeaders(HTTP_STATUS_BAD_REQUEST, EMPTY_BODY_LENGTH);
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(HTTP_STATUS_NOT_FOUND, EMPTY_BODY_LENGTH);
                } catch (Exception e) {
                    exchange.sendResponseHeaders(HTTP_STATUS_INTERNAL_ERROR, EMPTY_BODY_LENGTH);
                }
            }
        }

        public static ErrorHandler fromDelegate(HttpHandler delegate) {
            return new ErrorHandler(delegate);
        }
    }
}
