package company.vk.edu.distrib.compute.mcfluffybottoms.clustering;

import java.util.List;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.mcfluffybottoms.clustering.McfluffybottomsKVCluster.HasherType;

public class McfluffybottomsConsistentKVClusterFactory extends KVClusterFactory {

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new McfluffybottomsKVCluster(ports, HasherType.CONSISTENT);
    }
    
}
