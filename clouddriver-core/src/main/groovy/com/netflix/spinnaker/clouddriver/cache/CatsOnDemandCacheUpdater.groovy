/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.clouddriver.cache

import com.netflix.spinnaker.cats.agent.Agent
import com.netflix.spinnaker.cats.agent.AgentLock
import com.netflix.spinnaker.cats.agent.AgentScheduler
import com.netflix.spinnaker.cats.module.CatsModule
import com.netflix.spinnaker.cats.provider.Provider
import com.netflix.spinnaker.clouddriver.cache.OnDemandAgent.OnDemandType
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import java.util.concurrent.TimeUnit

@Component
@Slf4j
class CatsOnDemandCacheUpdater implements OnDemandCacheUpdater {

  private final List<Provider> providers
  private final CatsModule catsModule

  @Autowired
  AgentScheduler agentScheduler

  @Autowired
  CatsOnDemandCacheUpdater(List<Provider> providers, CatsModule catsModule) {
    this.providers = providers
    this.catsModule = catsModule
  }

  private Collection<OnDemandAgent> getOnDemandAgents() {
    providers.collect {
      it.agents.findAll { it instanceof OnDemandAgent } as Collection<OnDemandAgent>
    }.flatten()
  }

  @Override
  boolean handles(OnDemandType type, String cloudProvider) {
    onDemandAgents.any { it.handles(type, cloudProvider) }
  }

  @Override
  boolean handles(OnDemandType type, String cloudProvider, Map<String, ?> data) {
    return onDemandAgents.any { it.handles(type, cloudProvider, data) }
  }

  @Override
  boolean supportsBulk() {
    return onDemandAgents.every { it.supportsBulk() }
  }

  @Override
  OnDemandCacheUpdater.OnDemandCacheStatus handle(OnDemandType type, String cloudProvider, Map<String, ? extends Object> data) {
    handleBulk(type, cloudProvider, Collections.singletonList(data))
  }

  @Override
  OnDemandCacheUpdater.OnDemandCacheStatus handleBulk(OnDemandType type, String cloudProvider, List<Map<String, ?>> data) {
    if (supportsBulk()) {
      def nullAgent = new Object()

      def agentBulkData = data.groupBy { d ->
        def allAgents = onDemandAgents.findAll { it.handles(type, cloudProvider, d) }
        if (allAgents.size() > 1) {
          throw new IllegalStateException("Multple agents for $d")
        }
        allAgents ? allAgents[0] : nullAgent
      }
      agentBulkData.remove(nullAgent)

      def result = OnDemandCacheUpdater.OnDemandCacheStatus.SUCCESSFUL
      agentBulkData.each { OnDemandAgent agent, List<Map<String, ?>> bulkData ->
        def agentResult = handle(type, [agent], bulkData)
        if (agentResult == OnDemandCacheUpdater.OnDemandCacheStatus.PENDING) {
          result = agentResult
        }
      }
      return result
    } else if (data.size() > 1) {
      throw new IllegalStateException("bulk handle not supported")
    } else {
      Collection<OnDemandAgent> onDemandAgents = onDemandAgents.findAll { it.handles(type, cloudProvider, data[0]) }
      return handle(type, onDemandAgents, data)
    }
  }


  OnDemandCacheUpdater.OnDemandCacheStatus handle(OnDemandType type, Collection<OnDemandAgent> onDemandAgents, List<Map<String, ? extends Object>> data) {
    boolean hasOnDemandResults = false
    for (OnDemandAgent agent : onDemandAgents) {
      try {
        AgentLock lock = null
        if (agentScheduler.atomic && !(lock = agentScheduler.tryLock((Agent) agent))) {
          hasOnDemandResults = true // force Orca to retry
          continue
        }
        final long startTime = System.nanoTime()
        def providerCache = catsModule.getProviderRegistry().getProviderCache(agent.providerName)
        OnDemandAgent.OnDemandResult result = agent.handleBulk(providerCache, data)
        if (result) {
          if (agentScheduler.atomic && !(agentScheduler.lockValid(lock))) {
            hasOnDemandResults = true // force Orca to retry
            continue
          }
          if (result.cacheResult) {
            hasOnDemandResults = !(result.cacheResult.cacheResults ?: [:]).values().flatten().isEmpty() && !agentScheduler.atomic
            agent.metricsSupport.cacheWrite {
              providerCache.putCacheResult(result.sourceAgentType, result.authoritativeTypes, result.cacheResult)
            }
          }
          if (result.evictions) {
            agent.metricsSupport.cacheEvict {
              result.evictions.each { String evictType, Collection<String> ids ->
                providerCache.evictDeletedItems(evictType, ids)
              }
            }
          }
          if (agentScheduler.atomic && !(agentScheduler.tryRelease(lock))) {
            throw new IllegalStateException("We likely just wrote stale data. If you're seeing this, file a github issue: https://github.com/spinnaker/spinnaker/issues")
          }
          final long elapsed = System.nanoTime() - startTime
          agent.metricsSupport.recordTotalRunTimeNanos(elapsed)
          log.info("$agent.providerName/$agent.onDemandAgentType handled $type in ${TimeUnit.NANOSECONDS.toMillis(elapsed)} millis. Payload: $data")
        }
      } catch (e) {
        agent.metricsSupport.countError()
        log.warn("$agent.providerName/$agent.onDemandAgentType failed to handle on demand update for $type", e)
      }
    }

    return hasOnDemandResults ? OnDemandCacheUpdater.OnDemandCacheStatus.PENDING : OnDemandCacheUpdater.OnDemandCacheStatus.SUCCESSFUL
  }

  @Override
  Collection<Map> pendingOnDemandRequests(OnDemandType type, String cloudProvider) {
    if (agentScheduler.atomic) {
      return []
    }

    Collection<OnDemandAgent> onDemandAgents = onDemandAgents.findAll { it.handles(type, cloudProvider) }
    return onDemandAgents.collect {
      def providerCache = catsModule.getProviderRegistry().getProviderCache(it.providerName)
      it.pendingOnDemandRequests(providerCache)
    }.flatten()
  }
}
