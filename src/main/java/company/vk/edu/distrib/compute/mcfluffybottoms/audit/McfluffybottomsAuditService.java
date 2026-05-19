package company.vk.edu.distrib.compute.mcfluffybottoms.audit;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;

public class McfluffybottomsAuditService implements AuditService {
    private static final Logger log = LoggerFactory.getLogger(McfluffybottomsAuditService.class);

    private static final String TOPIC = "audit";
    private static final Duration TIMEOUT = Duration.ofMillis(200);

    private final String url;
    private final String bootstrapServers;
    private final String consumerGroupId;

    private Thread thread;
    private Connection connection;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public McfluffybottomsAuditService(String bootstrapServers, String consumerGroupId) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId.replaceAll("[^a-zA-Z0-9_-]", "_");
        this.url = "jdbc:h2:file:./mcfluffybottoms-audit-" + this.consumerGroupId + ";DB_CLOSE_DELAY=-1";
    }

    @Override
    public void start() {
        if (!started.compareAndSet(true, true)) {
            throw new IllegalStateException("Audit Service was already started, groupId=" + consumerGroupId);
        }

        try {
            initDatabase();
        } catch (SQLException e) {
            started.set(false);
            throw new IllegalStateException("Failed to create a database", e);
        }
        getThread();

        log.info("Audit Service started, groupId={}", consumerGroupId);
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }

        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        closeDatabase();
        log.info("Audit Service was stopped, groupId={}", consumerGroupId);
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        List<AuditEvent> events = new ArrayList<>();
        String sqlQuery = "SELECT method, entity_id, timestamp FROM events ORDER BY id";
        try (
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(sqlQuery)) {
            while (result.next()) {
                events.add(new AuditEvent(
                        result.getString("method"),
                        result.getString("entity_id"),
                        result.getLong("timestamp")));
            }
        } catch (SQLException e) {
            log.error("Error while listing audit entries", e);
        }
        throw new UnsupportedOperationException("Unimplemented method 'listAuditEntries'");
    }

    private void initDatabase() throws SQLException {
        this.connection = DriverManager.getConnection(url, "sa", "");
        String sqlQuery = """
                    CREATE TABLE IF NOT EXISTS events (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        method VARCHAR(10) NOT NULL,
                        entity_id VARCHAR(2048) NOT NULL,
                        timestamp BIGINT NOT NULL
                    )
                """;

        try (Statement statement = connection.createStatement()) {
            statement.execute(sqlQuery);
        }

        log.debug("Database initialized.");
    }

    private void closeDatabase() {
        if (connection == null) {
            return;
        }

        try {
            connection.close();
        } catch (SQLException e) {
            log.error("Error qhile closing database, {}", e.getMessage(), e);
        }
    }

    private void getThread() {
        this.thread = new Thread(this::consumerLoop, "audit-consumer-" + consumerGroupId);
        this.thread.setDaemon(true);
        this.thread.start();

        log.info("mcfluffybottoms Audit Service started: consumerGroupId={}", consumerGroupId);
    }

    private void consumerLoop() {
        try (KafkaConsumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(List.of(TOPIC));
            while (started.get() && !Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(TIMEOUT);
                if (!records.isEmpty()) {
                    processRecords(records);
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            if (started.get()) {
                log.error("Consumer loop failed", e);
            }
        }
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();

        properties.putAll(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));

        return new KafkaConsumer<>(properties);
    }

    private void processRecords(ConsumerRecords<String, String> records) throws SQLException {
        String sqlQuery = """
                    INSERT INTO events (method, entity_id, timestamp) VALUES (?, ?, ?)
                """;

        try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
            for (ConsumerRecord<String, String> record : records) {
                AuditEvent event = AuditEventSerializerUtils.deserialize(record.value());
                if (event != null) {
                    statement.setString(1, event.method());
                    statement.setString(2, event.id());
                    statement.setLong(3, event.timestamp());
                    statement.addBatch();
                    log.debug("Add statement {}", statement);
                }
            }
            statement.executeBatch();
            log.debug("Saved {} audit events", records.count());
        } catch (SQLException e) {
            log.error("Could not save audit events: {}", e.getMessage(), e);
        }
    }

}
