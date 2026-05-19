package company.vk.edu.distrib.compute.kirillmedvedev23;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public class KirillmedvedevFileSystemKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KirillmedvedevKVService(port, new KirillmedvedevFileSystemDao(Path.of("kirillmedvedev23-storage")));
    }
}
