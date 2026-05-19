package company.vk.edu.distrib.compute.kirillmedvedev23;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class ServerRunner {
    private static final Logger log = LoggerFactory.getLogger(ServerRunner.class);

    private ServerRunner() {
    }

    public static void main(String[] args) throws IOException {
        int port = 8080;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                log.error("Invalid port: {}", args[0]);
                System.exit(1);
            }
        }

        KVServiceFactory factory = new KirillmedvedevKVServiceFactory();
        KVService service = factory.create(port);
        service.start();
        log.info("Server started on port {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(service::stop));
    }
}
