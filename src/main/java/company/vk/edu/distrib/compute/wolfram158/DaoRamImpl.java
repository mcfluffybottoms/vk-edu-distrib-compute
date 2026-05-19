package company.vk.edu.distrib.compute.wolfram158;

import company.vk.edu.distrib.compute.Dao;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class DaoRamImpl implements Dao<byte[]> {
    private final Map<String, byte[]> keyToValue;

    public DaoRamImpl() {
        keyToValue = new ConcurrentHashMap<>();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        Utils.assertNotNulls(key);
        if (!keyToValue.containsKey(key)) {
            throw new NoSuchElementException();
        }
        return keyToValue.get(key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException {
        Utils.assertNotNulls(key, value);
        keyToValue.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        Utils.assertNotNulls(key);
        keyToValue.remove(key);
    }

    @Override
    public void close() {
        // nothing to close
    }
}
