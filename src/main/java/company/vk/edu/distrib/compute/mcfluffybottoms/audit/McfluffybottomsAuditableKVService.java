package company.vk.edu.distrib.compute.mcfluffybottoms.audit;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.Dao;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

public class McfluffybottomsAuditableKVService implements AuditableKVService {
    private static final Logger log = LoggerFactory.getLogger(McfluffybottomsAuditableKVService.class);
    private final Dao<byte[]> dao;

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";

    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";

    private final AtomicReference<String> bootstrapServers = new AtomicReference<>();
    private KafkaProducer<String, String> producer;
    private static final String TOPIC = "audit";

    private final AtomicBoolean isAsync = new AtomicBoolean(false);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private HttpServer server;
    private final int port;

    public McfluffybottomsAuditableKVService(int port, Dao<byte[]> dao) {
        this.dao = dao;
        this.port = port;
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Service was already started.");
        }
        initServer(port);
        server.start();
        log.info("mcfluffybottomsAuditableKVService started");
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            throw new IllegalStateException("Service was not started.");
        }
        server.stop(0);
        endProducer();
        try {
            dao.close();
        } catch (IOException e) {
            log.warn("Error closing DAO", e);
        }
        log.info("mcfluffybottomsAuditableKVService stopped");
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers.set(bootstrapServers);
    }

    @Override
    public void setAsync(boolean enabled) {
        isAsync.set(enabled);
    }

    private void initServer(int port) {

        if (bootstrapServers.get() != null) {
            createProducer();
        }

        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
            this.server.createContext(PATH_STATUS, this::handleStatus);
            this.server.createContext(PATH_ENTITY, this::handleEntity);
        } catch (IOException e) {
            log.error("Failed to create HTTP server on port {}.", port, e);
            endProducer();
            throw new IllegalStateException("Server initialization failed", e);
        }
    }

    private void createProducer() {
        if (bootstrapServers.get() == null) {
            return;
        }

        Properties properties = new Properties();

        properties.putAll(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.get(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.RETRIES_CONFIG, 3,
                ProducerConfig.LINGER_MS_CONFIG, 5));

        this.producer = new KafkaProducer<>(properties);
    }

    private void endProducer() {
        if (producer != null) {
            this.producer.flush();
            this.producer.close();
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

    private Map<String, String> getQueryData(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            exchange.sendResponseHeaders(400, 0);
            return new ConcurrentHashMap<>();
        }

        return parseQuery(query);
    }

    private void handleMethod(HttpExchange exchange, String method, String id) throws IOException {
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

    private void sendAuditEvent(String timestamp, String method, String id) throws IOException {
        if (producer == null) {
            return;
        }

        String encoded = AuditEventSerializerUtils.serialize(method, id, timestamp);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, id, encoded);

        if (isAsync.get()) {
            sendToProducerAsync(method, id, record);
        } else {
            sendToProducerSync(method, id, record);
        }
    }

    private void sendToProducerAsync(String method, String id, ProducerRecord<String, String> record) {
        this.producer.send(record, (data, e) -> {
            if (e != null) {
                log.warn(
                        "SendAuditEvent in async mode failed: method={}, id={}", method, id, e);
            }
        });
    }

    private void sendToProducerSync(String method, String id, ProducerRecord<String, String> record) {
        try {
            this.producer.send(record).get();
        } catch (InterruptedException e) {
            log.error(
                    "SendAuditEvent in sync mode failed: method={}, id={}", method, id, e);
        } catch (ExecutionException e) {
            Thread.currentThread().interrupt();
            log.warn(
                    "SendAuditEvent in async mode failed (thread interrupted): method={}, id={}", method, id, e);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            long timestamp = System.currentTimeMillis();
            String method = exchange.getRequestMethod();

            Map<String, String> args = getQueryData(exchange);
            if (args.isEmpty()) {
                log.error("Query is empty.");
                return;
            }

            String id = args.get("id");
            if (id == null || id.isBlank()) {
                log.error("No id present in the query.");
                exchange.sendResponseHeaders(400, 0);
                return;
            }

            sendAuditEvent(String.valueOf(timestamp), method, id);
            handleMethod(exchange, method, id);
            log.debug("Entity handled.");
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

}
