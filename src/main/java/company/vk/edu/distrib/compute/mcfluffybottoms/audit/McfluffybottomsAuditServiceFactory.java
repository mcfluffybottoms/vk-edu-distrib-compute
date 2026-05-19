package company.vk.edu.distrib.compute.mcfluffybottoms.audit;

import java.io.IOException;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.AuditServiceFactory;

public class McfluffybottomsAuditServiceFactory extends AuditServiceFactory {

    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) throws IOException {
        return new McfluffybottomsAuditService(bootstrapServers, consumerGroupId);
    }

}
