package com.netflix.spinnaker.clouddriver.cache;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import com.netflix.spinnaker.clouddriver.cache.OnDemandAgent.OnDemandType;

/**
 * An on-demand cache updater. Allows some non-scheduled trigger to initiate a cache refresh for a given type. An on-demand cache request will fan-out to all available updaters.
 */
public interface OnDemandCacheUpdater {
    /**
     * Indicates if the updater is able to handle this on-demand request given the type and cloudProvider
     */
    boolean handles(OnDemandType type, String cloudProvider);

    default boolean handles(OnDemandType type, String cloudProvider, Map<String, ?> data) {
        return handles(type, cloudProvider);
    }

    default boolean supportsBulk() {
        return false;
    }

    /**
     * Handles the update request
     */
    OnDemandCacheStatus handle(OnDemandAgent.OnDemandType type, String cloudProvider, Map<String, ?> data);

    OnDemandCacheStatus handleBulk(OnDemandAgent.OnDemandType type, String cloudProvider, List<Map<String, ?>> data);

    Collection<Map> pendingOnDemandRequests(OnDemandAgent.OnDemandType type, String cloudProvider);

    enum OnDemandCacheStatus {
        SUCCESSFUL, PENDING;
    }
}
