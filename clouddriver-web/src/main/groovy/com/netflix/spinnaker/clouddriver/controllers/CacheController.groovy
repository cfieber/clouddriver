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

package com.netflix.spinnaker.clouddriver.controllers

import com.netflix.spinnaker.clouddriver.cache.OnDemandAgent
import com.netflix.spinnaker.clouddriver.cache.OnDemandCacheQueue
import com.netflix.spinnaker.clouddriver.cache.OnDemandCacheUpdater
import com.netflix.spinnaker.clouddriver.cache.OnDemandConfigurationProperties
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/cache")
class CacheController {

  @Autowired
  List<OnDemandCacheUpdater> onDemandCacheUpdaters

  @Autowired
  OnDemandCacheQueue onDemandCacheQueue

  @Autowired
  OnDemandConfigurationProperties onDemandConfigurationProperties

  @RequestMapping(method = RequestMethod.POST, value = "/{cloudProvider}/{type}")
  ResponseEntity handleOnDemand(@PathVariable String cloudProvider,
                                @PathVariable String type,
                                @RequestBody Map<String, ?> data) {
    OnDemandAgent.OnDemandType onDemandType = OnDemandAgent.OnDemandType.fromString(type)
    def cacheStatus = OnDemandCacheUpdater.OnDemandCacheStatus.PENDING
    if (enableQueueing()) {
      onDemandCacheQueue.enqueue(cloudProvider, onDemandType, data)
    } else {
      cacheStatus = onDemandCacheUpdaters.find { it.handles(onDemandType, cloudProvider, data) }?.handle(onDemandType, cloudProvider, data)
    }
    def httpStatus = (cacheStatus == OnDemandCacheUpdater.OnDemandCacheStatus.PENDING) ? HttpStatus.ACCEPTED : HttpStatus.OK
    return new ResponseEntity(httpStatus)
  }

  @RequestMapping(method = RequestMethod.GET, value = "/{cloudProvider}/{type}")
  Collection<Map>  pendingOnDemands(@PathVariable String cloudProvider,
                                    @PathVariable String type) {
    OnDemandAgent.OnDemandType onDemandType = OnDemandAgent.OnDemandType.fromString(type)
    List<Map> inFlights = onDemandCacheUpdaters.findAll {
      it.handles(onDemandType, cloudProvider)
    }.collect {
      it.pendingOnDemandRequests(onDemandType, cloudProvider)
    }.flatten()
    if (enableQueueing()) {
      def queued = onDemandCacheQueue.currentQueue.collect {
        [
            details: it.data + [provider: it.cloudProvider, type: it.type.name()]
        ]
      }
      inFlights.addAll(queued)
    }
    return inFlights
  }

  @ExceptionHandler
  @ResponseStatus(HttpStatus.NOT_FOUND)
  Map handleOnDemandTypeNotFound(IllegalArgumentException ex) {
    [error: "cache.type.not.found", message: "Cache update type not found. Exception: ${ex.getMessage()}", status: HttpStatus.NOT_FOUND]
  }

  private boolean enableQueueing() {
    return onDemandConfigurationProperties.queuingEnabled && onDemandCacheUpdaters.every { it.supportsBulk() }
  }
}
