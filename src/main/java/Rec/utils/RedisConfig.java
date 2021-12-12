package Rec.utils;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;

public class RedisConfig {
    public RedisConfig(){}

    static {
        props = RedisConnProperties.getNodes();
        createClients();
    }

    private static JedisSentinelPool[] clients;

    private static String props;

    private static RedisNode[] confToNode(String nodes){
        String[] allNodeConfig = nodes.split(";");
        RedisNode[] redisNodes = new RedisNode[allNodeConfig.length];
        for (int i = 0;i< allNodeConfig.length;i++){
            redisNodes[i] = new RedisNode();
            redisNodes[i].setMaster(allNodeConfig[i].split("/")[1]);
            redisNodes[i].setSentinels(allNodeConfig[i].split("/")[0].split(","));
        }
        return redisNodes;
    }

    private static class RedisNode{
        private String[] sentinels;
        private String master;

        public String[] getSentinels() {
            return sentinels;
        }

        public void setSentinels(String[] sentinels) {
            this.sentinels = sentinels;
        }

        public String getMaster() {
            return master;
        }

        public void setMaster(String master) {
            this.master = master;
        }
    }

    private static void createClients(){
        RedisNode[] redisNodes = confToNode(props);
        clients = new JedisSentinelPool[redisNodes.length];
        for (int i = 0;i< redisNodes.length;i++){
            JedisPoolConfig config = jedisPoolConfig();
            clients[i] = new JedisSentinelPool(redisNodes[i].master,new HashSet<>(Arrays.asList(redisNodes[i].sentinels)),config);
        }
    }

    private static JedisPoolConfig jedisPoolConfig(){
        JedisPoolConfig jps = new JedisPoolConfig();
        jps.setMaxTotal(800);
        jps.setMaxIdle(400);
        jps.setMinIdle(100);
        jps.setBlockWhenExhausted(false);
        return jps;
    }

    public static JedisSentinelPool getPool(String key){
        String[] arr = key.split(":");
        int index = 0;
        if(arr.length > 0 && StringUtil.isNumeric(arr[0]) ){
            Integer userid = Integer.parseInt(Arrays.asList(arr).stream().map(s -> s.trim()).collect(Collectors.toList()).get(0));
            index = userid % size();
        }
        return clients[index];
    }

    public static int size() {return clients.length;}
}
