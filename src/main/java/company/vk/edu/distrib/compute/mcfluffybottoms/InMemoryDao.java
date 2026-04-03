package company.vk.edu.distrib.compute.mcfluffybottoms;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import company.vk.edu.distrib.compute.Dao;

public class InMemoryDao implements Dao<byte[]> {

    private final ConcurrentHashMap<String, byte[]> data = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (!data.containsKey(key)) {
            throw new NoSuchElementException("Key '" + key + "' not found.");
        }

        return data.get(key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        data.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        data.remove(key);
    }

    @Override
    public void close() throws IOException {
        data.clear();
    }

}
