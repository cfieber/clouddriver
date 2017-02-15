package com.netflix.spinnaker.clouddriver.cache;

import com.netflix.spinnaker.cats.agent.CacheResult;
import com.netflix.spinnaker.cats.provider.ProviderCache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface OnDemandAgent {
    String getProviderName();

    String getOnDemandAgentType();

    OnDemandMetricsSupport getMetricsSupport();

    boolean handles(OnDemandType type, String cloudProvider);

    default boolean handles(OnDemandType type, String cloudProvider, Map<String, ?> data) {
        return handles(type, cloudProvider);
    }

    default boolean supportsBulk() {
        return false;
    }

    OnDemandResult handle(ProviderCache providerCache, Map<String, ?> data);

    default OnDemandResult handleBulk(ProviderCache providerCache, List<Map<String, ?>> data) {
        if (!supportsBulk() && data.size() > 0) {
            throw new IllegalStateException(getOnDemandAgentType() + " does not support bulk on demand update");
        }
        return handle(providerCache, data.get(0));
    }

    Collection<Map> pendingOnDemandRequests(ProviderCache providerCache);

    enum OnDemandType {
        ServerGroup, SecurityGroup, LoadBalancer, Job;

        public static OnDemandType fromString(final String s) {
            return Arrays.stream(values())
                    .filter((t) -> t.name().equalsIgnoreCase(s))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Cannot create OnDemandType from String \'" + s + "\'"));
        }

    }

    class OnDemandResult {

        public OnDemandResult() {
            this(null, null, null, null);
        }

        @SuppressWarnings("unchecked")
        public OnDemandResult(Map<String, ?> params) {
            this((String) params.get("sourceAgentType"),
                    (Collection<String>) params.get("authoritativeTypes"),
                    (CacheResult) params.get("cacheResult"),
                    (Map<String, Collection<String>>) params.get("evictions"));
        }

        public OnDemandResult(String sourceAgentType, Collection<String> authoritativeTypes, CacheResult cacheResult, Map<String, Collection<String>> evictions) {
            this.sourceAgentType = sourceAgentType;
            this.authoritativeTypes = authoritativeTypes == null ? new ArrayList<>() : authoritativeTypes;
            this.cacheResult = cacheResult;
            this.evictions = evictions == null ? new LinkedHashMap<>() : evictions;
        }


        public String getSourceAgentType() {
            return sourceAgentType;
        }

        public void setSourceAgentType(String sourceAgentType) {
            this.sourceAgentType = sourceAgentType;
        }

        public Collection<String> getAuthoritativeTypes() {
            return authoritativeTypes;
        }

        public void setAuthoritativeTypes(Collection<String> authoritativeTypes) {
            this.authoritativeTypes = authoritativeTypes;
        }

        public CacheResult getCacheResult() {
            return cacheResult;
        }

        public void setCacheResult(CacheResult cacheResult) {
            this.cacheResult = cacheResult;
        }

        public Map<String, Collection<String>> getEvictions() {
            return evictions;
        }

        public void setEvictions(Map<String, Collection<String>> evictions) {
            this.evictions = evictions;
        }

        private String sourceAgentType;
        private Collection<String> authoritativeTypes = new ArrayList<>();
        private CacheResult cacheResult;
        private Map<String, Collection<String>> evictions = new LinkedHashMap<String, Collection<String>>();
    }
}
