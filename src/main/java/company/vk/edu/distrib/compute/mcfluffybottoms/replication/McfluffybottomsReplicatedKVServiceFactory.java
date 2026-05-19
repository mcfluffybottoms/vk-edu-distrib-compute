package company.vk.edu.distrib.compute.mcfluffybottoms.replication;

import java.io.IOException;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

public class McfluffybottomsReplicatedKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceImpl(port);
    }
}
