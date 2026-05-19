package company.vk.edu.distrib.compute.kirillmedvedev23;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KirillmedvedevFileSystemDao implements Dao<byte[]> {
    private static final int MAX_VALUE_SIZE = 1024 * 1024;

    private final Path storageDir;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public KirillmedvedevFileSystemDao(Path storageDir) throws IOException {
        this.storageDir = storageDir;
        Files.createDirectories(storageDir);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        requireValidKey(key);
        lock.readLock().lock();
        try {
            Path filePath = storageDir.resolve(sanitizeFileName(key));
            try {
                return Files.readAllBytes(filePath);
            } catch (NoSuchFileException e) {
                NoSuchElementException ex = new NoSuchElementException("Key not found: " + key);
                ex.initCause(e);
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
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
        lock.writeLock().lock();
        try {
            Path filePath = storageDir.resolve(sanitizeFileName(key));
            Files.write(filePath, value, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        requireValidKey(key);
        lock.writeLock().lock();
        try {
            Path filePath = storageDir.resolve(sanitizeFileName(key));
            Files.deleteIfExists(filePath);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        // Storage directory is managed externally, no cleanup needed
    }

    private void requireValidKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key is null or empty");
        }
    }

    private static String sanitizeFileName(String key) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(key.getBytes(StandardCharsets.UTF_8));
    }
}
