package Rec.utils;

public class RedisConnProperties {
    private static String nodes = BasePropertiesUtils.getProperty("redis.node");

    public static String getNodes() {
        return nodes;
    }

    public static void setNodes(String nodes) {
        RedisConnProperties.nodes = nodes;
    }
}
