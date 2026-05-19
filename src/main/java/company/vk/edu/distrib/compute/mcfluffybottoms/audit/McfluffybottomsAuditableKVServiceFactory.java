package company.vk.edu.distrib.compute.mcfluffybottoms.audit;

import java.io.IOException;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.mcfluffybottoms.InMemoryDao;

public class McfluffybottomsAuditableKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new McfluffybottomsAuditableKVService(port, new InMemoryDao());
    }

}
