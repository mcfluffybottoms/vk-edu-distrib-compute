package company.vk.edu.distrib.compute.wolfram158;

import company.vk.edu.distrib.compute.Dao;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DaoFileWithCacheImpl implements Dao<byte[]> {
    private final ConcurrentMap<String, byte[]> keyToValueCache;
    private final Path dbPath;
    private final ConcurrentMap<String, ReentrantReadWriteLock> keyToLock;
    private static final String DB_EXTENSION = ".db";

    public DaoFileWithCacheImpl() throws IOException {
        keyToValueCache = new ConcurrentHashMap<>();
        dbPath = Paths.get("./storage");
        Files.createDirectories(dbPath);
        keyToLock = new ConcurrentHashMap<>();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        Utils.assertNotNulls(key);
        final byte[] value = keyToValueCache.get(key);
        if (value != null) {
            return value;
        }
        final Path filePath = addExtension(dbPath.resolve(key), DB_EXTENSION);
        if (!Files.exists(filePath)) {
            throw new NoSuchElementException();
        }
        final ReentrantReadWriteLock lock = keyToLock.computeIfAbsent(key, str -> new ReentrantReadWriteLock());
        lock.readLock().lock();
        try (InputStream is = Files.newInputStream(filePath)) {
            final DataInputStream dis = new DataInputStream(is);
            final int keyLength = dis.readInt();
            final int valueLength = dis.readInt();
            final byte[] keyBytes = new byte[keyLength];
            final int _ = dis.read(keyBytes, 0, keyLength);
            final byte[] valueBytes = new byte[valueLength];
            final int _ = dis.read(valueBytes, 0, valueLength);
            keyToValueCache.put(key, valueBytes);
            return valueBytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        Utils.assertNotNulls(key, value);
        final ReentrantReadWriteLock lock = keyToLock.computeIfAbsent(key, str -> new ReentrantReadWriteLock());
        final Path filePath = addExtension(dbPath.resolve(key), DB_EXTENSION);
        final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        lock.writeLock().lock();
        try (OutputStream os = Files.newOutputStream(filePath)) {
            final DataOutputStream dos = new DataOutputStream(os);
            dos.writeInt(keyBytes.length);
            dos.writeInt(value.length);
            dos.write(keyBytes);
            dos.write(value);
            dos.flush();
            keyToValueCache.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        Utils.assertNotNulls(key);
        final Path filePath = addExtension(dbPath.resolve(key), DB_EXTENSION);
        final ReentrantReadWriteLock lock = keyToLock.computeIfAbsent(key, str -> new ReentrantReadWriteLock());
        lock.writeLock().lock();
        try {
            Files.deleteIfExists(filePath);
            keyToValueCache.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // nothing to close
    }

    private Path addExtension(Path path, String ext) {
        final String fileName = path.getFileName().toString();
        final String newFileName = fileName + ext;
        return path.resolveSibling(newFileName);
    }
}
