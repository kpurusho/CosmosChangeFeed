package core;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

public class RedisCacheAccessor implements CacheAccessor {

    private final JedisShardInfo shardInfo;
    private final Jedis jedis;

    public RedisCacheAccessor(String cacheHost, String cacheKey) {
        shardInfo = new JedisShardInfo(cacheHost, 6380, true);
        shardInfo.setPassword(cacheKey);
        jedis = new Jedis(shardInfo);
    }
    @Override
    public void set(String key, String value) {
        jedis.set(key, value);
    }

    @Override
    public String get(String key) {
        return jedis.get(key);
    }
}
