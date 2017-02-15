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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.netflix.spinnaker.clouddriver.cache.OnDemandAgent.OnDemandType;


public interface OnDemandCacheQueue {
    class QueuedCacheRefresh {
        String cloudProvider;
        OnDemandType type;
        Map<String, ?> data;

        public QueuedCacheRefresh(String cloudProvider, OnDemandType type, Map<String, ?> data) {
            this.cloudProvider = Objects.requireNonNull(cloudProvider);
            this.type = Objects.requireNonNull(type);
            this.data = data == null ? Collections.emptyMap() : data;
        }

        public String getCloudProvider() {
            return cloudProvider;
        }

        public OnDemandType getType() {
            return type;
        }

        public Map<String, ?> getData() {
            return data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            QueuedCacheRefresh that = (QueuedCacheRefresh) o;

            if (!cloudProvider.equals(that.cloudProvider)) return false;
            if (type != that.type) return false;
            return data.equals(that.data);
        }

        @Override
        public int hashCode() {
            int result = cloudProvider.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + data.hashCode();
            return result;
        }
    }

    default void enqueue(String cloudProvider, String type, Map<String, ?> data) {
        enqueue(cloudProvider, OnDemandType.fromString(type), data);
    }

    void enqueue(String cloudProvider, OnDemandType type, Map<String, ?> data);

    List<QueuedCacheRefresh> getCurrentQueue();

    List<QueuedCacheRefresh> clearCurrentQueue();
}
