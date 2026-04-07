package company.vk.edu.distrib.compute.mcfluffybottoms;

import java.io.IOException;
import java.nio.file.Path;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

public class FileKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceImpl(port, new FileDao(Path.of("mcfluffybottomsdata")));
    }
}
