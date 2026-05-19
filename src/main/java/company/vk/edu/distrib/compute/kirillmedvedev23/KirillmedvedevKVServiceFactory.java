package company.vk.edu.distrib.compute.kirillmedvedev23;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class KirillmedvedevKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KirillmedvedevKVService(port, new KirillmedvedevDao());
    }
}
