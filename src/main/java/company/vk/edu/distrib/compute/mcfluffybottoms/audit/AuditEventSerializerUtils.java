package company.vk.edu.distrib.compute.mcfluffybottoms.audit;

import company.vk.edu.distrib.compute.AuditEvent;

public class AuditEventSerializerUtils {
    private AuditEventSerializerUtils() {
    }

    public static String serialize(String method, String id, String timestamp) {
        return method + "_" + id + "_" + timestamp;
    }

    public static AuditEvent deserialize(String event) {
        if (event == null) {
            return null;
        }
        String[] parts = event.split("_", 3);
        if (parts.length != 3) {
            return null;
        }

        return new AuditEvent(parts[0], parts[1], Long.parseLong(parts[2]));
    }
}
