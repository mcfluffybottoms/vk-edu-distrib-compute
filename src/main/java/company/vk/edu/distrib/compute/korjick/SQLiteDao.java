package company.vk.edu.distrib.compute.korjick;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.NoSuchElementException;

public class SQLiteDao implements Dao<byte[]> {

    private static final String CONNECTION_STRING = "jdbc:sqlite:./storage/cakekv.db";
    private static final String TABLE_NAME = "storage";
    private static final String KEY_COLUMN = "key";
    private static final String VALUE_COLUMN = "value";

    private static final String CREATE_TABLE_QUERY = String.format(
            "CREATE TABLE IF NOT EXISTS %s (%s TEXT PRIMARY KEY, %s BLOB);",
            TABLE_NAME, KEY_COLUMN, VALUE_COLUMN);
    private static final String SELECT_QUERY = String.format("SELECT %s FROM %s WHERE %s = ?;",
            VALUE_COLUMN, TABLE_NAME, KEY_COLUMN);
    private static final String INSERT_QUERY = String.format("INSERT OR REPLACE INTO %s (%s, %s) VALUES (?, ?);",
            TABLE_NAME, KEY_COLUMN, VALUE_COLUMN);
    private static final String DELETE_QUERY = String.format("DELETE FROM %s WHERE %s = ?;",
            TABLE_NAME, KEY_COLUMN);

    private static final Logger log = LoggerFactory.getLogger(SQLiteDao.class);

    private final Connection connection;

    public SQLiteDao() throws SQLException {
        connection = DriverManager.getConnection(CONNECTION_STRING);
        log.info("Connected to SQLite database");
        final var statement = connection.prepareStatement(CREATE_TABLE_QUERY);
        statement.executeUpdate();
        log.info("Storage initialized successfully");
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is null or blank");
        }
        try (var statement = connection.prepareStatement(SELECT_QUERY)) {
            statement.setString(1, key);

            final var resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getBytes(VALUE_COLUMN);
            } else {
                throw new NoSuchElementException("No value for key: " + key);
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is null or blank");
        }
        try (var statement = connection.prepareStatement(INSERT_QUERY)) {
            statement.setString(1, key);
            statement.setBytes(2, value);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is null or blank");
        }
        try (var statement = connection.prepareStatement(DELETE_QUERY)) {
            statement.setString(1, key);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
