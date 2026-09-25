package com.storm.iotdata.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;

/**
 * Redis Pub/Sub publisher for plug anomaly events.
 */
public class RedisAnomalyPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisAnomalyPublisher.class);

    private final String redisHost;
    private final int redisPort;
    private final ObjectMapper objectMapper;
    private transient JedisPool jedisPool;
    private boolean initialized;

    public RedisAnomalyPublisher() {
        this.redisHost = StormConfig.getRedisHost();
        this.redisPort = StormConfig.getRedisPort();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Initializes the Redis connection pool. Redis failure does not stop the bolt.
     */
    public void initialize() {
        try {
            jedisPool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort);
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.ping();
            }
            initialized = true;
            LOGGER.info("Connected Redis anomaly publisher to {}:{}", redisHost, redisPort);
        } catch (RuntimeException exception) {
            initialized = false;
            LOGGER.error("Failed to initialize Redis anomaly publisher at {}:{}", redisHost, redisPort, exception);
        }
    }

    /**
     * Publishes an anomaly payload to a Redis channel.
     *
     * @param channel Redis Pub/Sub channel.
     * @param event JSON-compatible event payload.
     */
    public void publish(String channel, Map<String, Object> event) {
        if (!initialized || jedisPool == null) {
            LOGGER.warn("Skipping Redis anomaly publish because publisher is not initialized for {}", channel);
            return;
        }

        try {
            String payload = objectMapper.writeValueAsString(event);
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.publish(channel, payload);
            }
            LOGGER.info("Published anomaly event to Redis channel {}", channel);
        } catch (JsonProcessingException exception) {
            LOGGER.error("Failed to serialize anomaly event for Redis channel {}", channel, exception);
        } catch (RuntimeException exception) {
            LOGGER.error("Failed to publish anomaly event to Redis channel {}", channel, exception);
        }
    }

    /**
     * Closes the Redis connection pool.
     */
    public void close() {
        if (jedisPool == null) {
            return;
        }
        try {
            jedisPool.close();
            LOGGER.info("Closed Redis anomaly publisher connection");
        } catch (RuntimeException exception) {
            LOGGER.warn("Failed to close Redis anomaly publisher cleanly", exception);
        } finally {
            jedisPool = null;
            initialized = false;
        }
    }
}
