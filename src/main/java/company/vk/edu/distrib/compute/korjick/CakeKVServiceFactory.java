package company.vk.edu.distrib.compute.korjick;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class CakeKVServiceFactory extends KVServiceFactory {

    public static final StorageType DEFAULT_STORAGE_TYPE = StorageType.IN_MEMORY;

    @Override
    protected KVService doCreate(int port) throws IOException {
        final Dao<byte[]> dao;
        if (DEFAULT_STORAGE_TYPE == StorageType.SQLITE) {
            try {
                dao = new SQLiteDao();
            } catch (Exception e) {
                throw new IOException(e);
            }
        } else {
            dao = new InMemoryDao();
        }

        return new CakeKVService(port, dao);
    }

    public enum StorageType {
        SQLITE, IN_MEMORY
    }
}
