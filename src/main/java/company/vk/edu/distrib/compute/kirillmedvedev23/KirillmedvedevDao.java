package company.vk.edu.distrib.compute.kirillmedvedev23;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class KirillmedvedevDao implements Dao<byte[]> {
    private static final int MAX_VALUE_SIZE = 1024 * 1024;
    
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        requireValidKey(key);
        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        requireValidKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        if (value.length > MAX_VALUE_SIZE) {
            throw new IllegalArgumentException("Value too large: max " + MAX_VALUE_SIZE + " bytes");
        }
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        requireValidKey(key);
        storage.remove(key);
    }

    @Override
    public void close() {
        // nothing to close
    }

    private void requireValidKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key is null or empty");
        }
    }
}
