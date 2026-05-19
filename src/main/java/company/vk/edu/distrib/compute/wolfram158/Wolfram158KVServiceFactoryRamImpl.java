package company.vk.edu.distrib.compute.wolfram158;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class Wolfram158KVServiceFactoryRamImpl extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceImpl(port, new DaoRamImpl());
    }
}
