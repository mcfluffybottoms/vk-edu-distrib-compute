package company.vk.edu.distrib.compute.luckyslon2003;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HexFormat;
import java.util.NoSuchElementException;

public class FileDao implements Dao<byte[]> {

    private final Path baseDir;

    public FileDao(Path baseDir) throws IOException {
        this.baseDir = baseDir;
        Files.createDirectories(baseDir);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);
        Path file = resolve(key);
        try {
            return Files.readAllBytes(file);
        } catch (NoSuchFileException e) {
            throw new NoSuchElementException("No entry for key: " + key, e);
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        Path file = resolve(key);
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.write(tmp, value);
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        Files.deleteIfExists(resolve(key));
    }

    @Override
    public void close() {
        // nothing to do
    }

    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }
    }

    private Path resolve(String key) {
        String hex = HexFormat.of().formatHex(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return baseDir.resolve(hex);
    }
}
