/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.clouddriver.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spectator.api.Registry;
import com.netflix.spinnaker.cats.redis.JedisSource;
import com.netflix.spinnaker.clouddriver.cache.OnDemandAgent.OnDemandType;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RedisOnDemandCacheQueue implements OnDemandCacheQueue {
    private static final String KEY_NAME = "onDemandQueue";

    private final JedisSource jedisSource;
    private final Registry registry;
    private final ObjectMapper mapper = new ObjectMapper();

    public RedisOnDemandCacheQueue(JedisSource jedisSource, Registry registry) {
        this.jedisSource = jedisSource;
        this.registry = registry;
    }

    @Override
    public void enqueue(String cloudProvider, OnDemandType type, Map<String, ?> data) {
        final QueuedCacheRefresh item = new QueuedCacheRefresh(cloudProvider, type, data);
        final String json;
        try {
            json = mapper.writeValueAsString(item);
        } catch (JsonProcessingException jpe) {
            registry.counter("onDemandCacheQueue.enqueueErrors").increment();
            throw new RuntimeException(jpe);
        }
        try (Jedis jedis = jedisSource.getJedis()) {
            registry.counter("onDemandCacheQueue.enqueue").increment();
            jedis.rpush(KEY_NAME, json);
        }
    }

    @Override
    public List<QueuedCacheRefresh> getCurrentQueue() {
        final List<String> items;
        try (Jedis jedis = jedisSource.getJedis()) {
            items = jedis.lrange(KEY_NAME, 0, -1);
        }
        return convertJson(items);
    }

    @Override
    public List<QueuedCacheRefresh> clearCurrentQueue() {
        String tempKey = UUID.randomUUID().toString();
        final List<String> items;
        try (Jedis jedis = jedisSource.getJedis()) {
            if (jedis.exists(KEY_NAME)) {
                jedis.rename(KEY_NAME, tempKey);
                items = jedis.lrange(tempKey, 0, -1);
                jedis.expire(tempKey, 1);
            } else {
                items = Collections.emptyList();
            }
        }
        return convertJson(items);
    }

    private List<QueuedCacheRefresh> convertJson(List<String> raw) {
        final List<QueuedCacheRefresh> qcr = new ArrayList<>(raw.size());
        for (String json : raw) {
            try {
                qcr.add(mapper.readValue(json, QueuedCacheRefresh.class));
            } catch (IOException ioe) {
                //ehh
            }
        }
        return qcr;
    }
}
