import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

public class RedisConnection {
    final JedisPoolConfig poolConfig = buildPoolConfig();

    private JedisPoolConfig buildPoolConfig() {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }
//    private Jedis jedis;
//
//    public RedisConnection() {
//            this.jedis = new Jedis("hostname", 6379);
//            this.jedis.auth("password");
//            System.out.println("Connected to Redis");
//    }
//
//    public Jedis getJedis() {
//        return this.jedis;
//    }
}
