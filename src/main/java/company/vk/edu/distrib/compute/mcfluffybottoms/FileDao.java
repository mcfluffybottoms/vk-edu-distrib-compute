package company.vk.edu.distrib.compute.mcfluffybottoms;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import company.vk.edu.distrib.compute.Dao;

public class FileDao implements Dao<byte[]> {

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Path root;

    public FileDao(Path root) throws IllegalArgumentException, IOException {
        if (root == null) {
            throw new IllegalArgumentException("Root path cannot be null.");
        }
        this.root = root;
        Files.createDirectories(root);
    }

    @Override
    public void close() throws IOException {
        closed.set(true);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkIfClosed();
        validateKey(key);

        Path path = getPathByKey(key);
        if (!Files.exists(path)) {
            throw new NoSuchElementException("Key '" + key + "' not found.");
        }
        return Files.readAllBytes(path);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkIfClosed();
        validateKey(key);
        validateValue(value);

        Path temp = getTempPathByKey(key);
        Files.write(temp, value);

        Files.move(
                temp, getPathByKey(key),
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE
        );
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkIfClosed();
        validateKey(key);
        Files.deleteIfExists(getPathByKey(key));
    }

    private Path getPathByKey(String key) {
        return root.resolve(key);
    }

    private Path getTempPathByKey(String key) {
        return root.resolve(key + ".temp");
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

    private void checkIfClosed() {
        if (closed.get()) {
            throw new IllegalStateException("Dao is closed.");
        }
    }

}
