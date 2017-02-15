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

package com.netflix.spinnaker.clouddriver.core.agent;

import com.netflix.spectator.api.Registry;
import com.netflix.spinnaker.cats.agent.RunnableAgent;
import com.netflix.spinnaker.clouddriver.cache.CustomScheduledAgent;
import com.netflix.spinnaker.clouddriver.cache.OnDemandAgent.OnDemandType;
import com.netflix.spinnaker.clouddriver.cache.OnDemandCacheQueue;
import com.netflix.spinnaker.clouddriver.cache.OnDemandCacheQueue.QueuedCacheRefresh;
import com.netflix.spinnaker.clouddriver.cache.OnDemandCacheUpdater;
import com.netflix.spinnaker.clouddriver.core.provider.CoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * ProcessOnDemandCacheQueueAgent.
 */
public class ProcessOnDemandCacheQueueAgent implements RunnableAgent, CustomScheduledAgent {
    private static final Logger log = LoggerFactory.getLogger(ProcessOnDemandCacheQueueAgent.class);

    private static final long DEFAULT_POLL_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(5);
    private static final long DEFAULT_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);

    private final OnDemandCacheQueue onDemandCacheQueue;
    private final List<OnDemandCacheUpdater> onDemandCacheUpdaters;
    private final Registry registry;
    private final long pollIntervalMillis;
    private final long timeoutMillis;

    public ProcessOnDemandCacheQueueAgent(OnDemandCacheQueue onDemandCacheQueue,
                                          List<OnDemandCacheUpdater> onDemandCacheUpdaters,
                                          Registry registry) {
        this(onDemandCacheQueue, onDemandCacheUpdaters, registry, DEFAULT_POLL_INTERVAL_MILLIS, DEFAULT_TIMEOUT_MILLIS);
    }

    public ProcessOnDemandCacheQueueAgent(OnDemandCacheQueue onDemandCacheQueue,
                                          List<OnDemandCacheUpdater> onDemandCacheUpdaters,
                                          Registry registry,
                                          long pollIntervalMillis,
                                          long timeoutMillis) {
        this.onDemandCacheQueue = onDemandCacheQueue;
        this.onDemandCacheUpdaters = onDemandCacheUpdaters;
        this.registry = registry;
        this.pollIntervalMillis = pollIntervalMillis;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public String getAgentType() {
        return ProcessOnDemandCacheQueueAgent.class.getSimpleName();
    }

    @Override
    public String getProviderName() {
        return CoreProvider.PROVIDER_NAME;
    }

    @Override
    public void run() {
        List<QueuedCacheRefresh> queue = onDemandCacheQueue.clearCurrentQueue();
        Map<String, Map<OnDemandType, Collection<QueuedCacheRefresh>>> byProviderAndType = new HashMap<>();
        for (QueuedCacheRefresh item : queue) {
            Map<OnDemandType, Collection<QueuedCacheRefresh>> typeMap = byProviderAndType.computeIfAbsent(item.getCloudProvider(), (k) -> new HashMap<>());
            typeMap.computeIfAbsent(item.getType(), (k) -> new LinkedHashSet<>()).add(item);
        }

        byProviderAndType.entrySet().forEach(p -> {
            final String provider = p.getKey();
            p.getValue().entrySet().forEach(t -> {
                final OnDemandType type = t.getKey();
                for (QueuedCacheRefresh qcr : t.getValue()) {
                    onDemandCacheUpdaters.stream()
                            .filter(odp -> odp.handles(type, provider))
                            .forEach(odp -> odp.handle(type, provider, qcr.getData()));
                }
            });
        });
    }

    @Override
    public long getPollIntervalMillis() {
        return pollIntervalMillis;
    }

    @Override
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

}
