package com.clarke.CacheTest.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.redis.cache.DefaultRedisCachePrefix;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.cache.RedisCachePrefix;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.support.collections.DefaultRedisSet;
import org.springframework.data.redis.support.collections.RedisSet;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.Executor;

@Configuration
public class CommonRedisConfig {

    public static final String CACHE_MYAPP_CACHE_NAMES = "cache:myapp:cacheNames";

    @Value("${spring.redis.port:6379}")
    private int redisPort;

    @Value("${spring.redis.host:localhost}")
    private String redisHostName;

    @Value("${spring.redis.password:}")
    private String redisPassword;

    @Value("${spring.redis.taskExecutorMaxThreads:20}")
    private int redisTaskExecutorMaxThreads;

    @Bean
    RedisConnectionFactory connectionFactory() {
        JedisConnectionFactory factory = new JedisConnectionFactory();
        factory.setHostName(redisHostName);
        factory.setPort(redisPort);
        factory.setPassword(redisPassword);
        factory.setUsePool(true);
        return factory;
    }

    @Bean
    @Qualifier("json-redis-template")
    RedisTemplate<String,Object> getRedisTemplate(@Autowired RedisConnectionFactory connectionFactory){
        final RedisTemplate< String, Object > template =  new RedisTemplate<>();
        template.setConnectionFactory( connectionFactory );
        template.setKeySerializer( new StringRedisSerializer() );
        template.setHashValueSerializer( new GenericJackson2JsonRedisSerializer() );
        template.setValueSerializer( new GenericJackson2JsonRedisSerializer() );
        template.afterPropertiesSet();

        return template;
    }

    @Bean
    @Qualifier("state-redis-manager")
    public RedisCacheManager redisCacheManager(@Qualifier("json-redis-template") RedisTemplate<String,Object> jsonRedisTemplate){
        RedisCacheManager redisCacheManager;

        redisCacheManager = new RedisCacheManager(jsonRedisTemplate);

        // This Section will Ensure the Keys are Unique by adding the CacheName to the details.
        RedisCachePrefix redisCachePrefix = new StateRedisCachePrefix();
        redisCacheManager.setUsePrefix(true);
        redisCacheManager.setCachePrefix(redisCachePrefix);

        // This Section will reload the Data from Redis on Startup.
        redisCacheManager.setLoadRemoteCachesOnStartup(true);

        // This Call will Stop the data being deleted by Redis after a certain Time. (isEternal==true)
        // We may need to add a job to clear the Redis DB at regular intervals.
        redisCacheManager.setDefaultExpiration(0);

        return redisCacheManager;
    }
    // Overriding the default SimpleAsyncTaskExecutor used by Spring Redis
    // It creates a new short-lived thread for every message/event. Which means a lot of thread creation overhead.
    // This maxThreads number for ThreadPoolTaskExecutor will need tuned as part of performance testing.
    @Bean
    Executor redisTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(redisTaskExecutorMaxThreads);
        taskExecutor.setThreadNamePrefix("redis-task-executor-");
        taskExecutor.initialize();
        return taskExecutor;
    }

    @Bean
    @Qualifier("state-name-set")
    Set<String> stateNameSet(@Autowired RedisConnectionFactory connectionFactory){
        StringRedisTemplate srt = new StringRedisTemplate(connectionFactory);
        RedisSet<String> stateNameSet = new DefaultRedisSet<String>(CACHE_MYAPP_CACHE_NAMES,srt);
        return stateNameSet;
    }

    @Bean
    RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory, Executor redisTaskExecutor) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setTaskExecutor(redisTaskExecutor);
        container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor("redis-sub-executor-"));
        return container;
    }

    public static class StateRedisCachePrefix implements RedisCachePrefix {

        private final String extraPrefix;
        private final RedisSerializer serializer;
        private final String delimiter;


        public StateRedisCachePrefix(){
            this(":","cache");
        }

        public StateRedisCachePrefix(String delimiter,String extraPrefix) {
            if(null==delimiter || null==extraPrefix){
                throw new IllegalArgumentException("Delimiter and Prefix Required");
            }
            this.serializer = new StringRedisSerializer();
            this.delimiter = delimiter;
            this.extraPrefix = extraPrefix;
        }


        public byte[] prefix(String cacheName) {
            return this.serializer.serialize(extraPrefix.concat(this.delimiter).concat(cacheName).concat(this.delimiter));
        }



    }

}
