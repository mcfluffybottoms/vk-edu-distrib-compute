package company.vk.edu.distrib.compute.mcfluffybottoms;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import company.vk.edu.distrib.compute.Dao;

public class InMemoryDao implements Dao<byte[]> {

    private final Map<String, byte[]> data = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);

        if (!data.containsKey(key)) {
            throw new NoSuchElementException("Key '" + key + "' not found.");
        }

        return data.get(key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        validateValue(value);
        data.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        data.remove(key);
    }

    @Override
    public void close() throws IOException {
        data.clear();
    }

    private void validateKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key is null.");
        }
    }

    private void validateValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Value is null.");
        }
    }
}
