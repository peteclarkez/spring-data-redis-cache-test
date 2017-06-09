package com.clarke.CacheTest.service;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.data.redis.cache.DefaultRedisCachePrefix;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.*;

import javax.websocket.server.PathParam;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@RestController

public class CacheQueryService {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Set<String> stateNameSet;
    private final RedisTemplate<String,Object> jsonRedisTemplate;
    private final RedisCacheManager redisCacheManager;

    @Autowired
    public CacheQueryService(@Qualifier("json-redis-template") RedisTemplate<String,Object> jsonRedisTemplate,
                             @Qualifier("state-redis-manager") RedisCacheManager redisCacheManager,
                             @Qualifier("state-name-set") Set<String> stateNameSet){
        this.jsonRedisTemplate = jsonRedisTemplate;
        this.redisCacheManager = redisCacheManager;
        this.stateNameSet = stateNameSet;

        log.info("Redis State Manager Startup Complete:");
    }

    @RequestMapping("/cache")
    public Collection<String> handleCache(){
        return stateNameSet;
    }

    @GetMapping(path = "/cache/{cacheKey}")
    public Object handleCacheQuery(@PathVariable String cacheKey ){
        Cache cache = redisCacheManager.getCache(cacheKey);
        stateNameSet.add(cacheKey);
        return null!=cache?cache.getName():null;
    }

    @RequestMapping(path = "/cache/{cacheKey}", method = RequestMethod.DELETE)
    public void handleCacheDelete(@PathVariable String cacheKey ){
        Cache cache = redisCacheManager.getCache(cacheKey);
        cache.clear();
        redisCacheManager.setExpires(ImmutableMap.of(cacheKey,1L));
        stateNameSet.remove(cacheKey);

    }

    @GetMapping(path = "/cache/{cacheKey}/{key}")
    public Object handleCacheQuery(@PathVariable String cacheKey, @PathVariable String key ){
        Cache cache = redisCacheManager.getCache(cacheKey);
        stateNameSet.add(cacheKey);
        if(null==cache){ return null;}
        return null!=cache.get(key)?cache.get(key).get():null;
    }


    @PostMapping("/cache/{cacheKey}/{key}")
    public void addCacheValue(@PathVariable String cacheKey, @PathVariable String key, @RequestBody  String value){
        Cache cache = redisCacheManager.getCache(cacheKey);
        stateNameSet.add(cacheKey);
        cache.put(key,value);
    }


}
