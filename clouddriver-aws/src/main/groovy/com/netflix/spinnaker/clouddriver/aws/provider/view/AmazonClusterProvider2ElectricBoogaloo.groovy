package com.netflix.spinnaker.clouddriver.aws.provider.view

import com.amazonaws.services.autoscaling.model.AutoScalingGroup
import com.amazonaws.services.autoscaling.model.LaunchConfiguration
import com.amazonaws.services.autoscaling.model.ScalingPolicy
import com.amazonaws.services.autoscaling.model.ScheduledUpdateGroupAction
import com.amazonaws.services.cloudwatch.model.MetricAlarm
import com.amazonaws.services.ec2.model.Image
import com.amazonaws.services.ec2.model.Instance
import com.amazonaws.services.ec2.model.Subnet
import com.amazonaws.services.elasticloadbalancing.model.LoadBalancerDescription
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroup
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.awsobjectmapper.AmazonObjectMapperConfigurer
import com.netflix.spinnaker.clouddriver.aws.AmazonCloudProvider
import com.netflix.spinnaker.clouddriver.aws.data.ArnUtils
import com.netflix.spinnaker.clouddriver.aws.model.AmazonCluster
import com.netflix.spinnaker.clouddriver.aws.model.AmazonInstance
import com.netflix.spinnaker.clouddriver.aws.model.AmazonLoadBalancer
import com.netflix.spinnaker.clouddriver.aws.model.AmazonServerGroup
import com.netflix.spinnaker.clouddriver.aws.model.AmazonTargetGroup
import com.netflix.spinnaker.clouddriver.aws.model.InstanceTargetGroupState
import com.netflix.spinnaker.clouddriver.aws.model.InstanceTargetGroups
import com.netflix.spinnaker.clouddriver.aws.model.edda.InstanceLoadBalancerState
import com.netflix.spinnaker.clouddriver.aws.model.edda.InstanceLoadBalancers
import com.netflix.spinnaker.clouddriver.aws.provider.agent.InstanceCachingAgent
import com.netflix.spinnaker.clouddriver.aws.provider.view.boogaloo.AccountRegionData
import com.netflix.spinnaker.clouddriver.aws.provider.view.boogaloo.RegionalCache
import com.netflix.spinnaker.clouddriver.eureka.model.EurekaInstance
import com.netflix.spinnaker.clouddriver.model.ClusterProvider
import com.netflix.spinnaker.clouddriver.model.Instance as InstanceModel
import com.netflix.spinnaker.clouddriver.model.LoadBalancerInstance
import com.netflix.spinnaker.clouddriver.model.LoadBalancerServerGroup
import com.netflix.spinnaker.clouddriver.model.ServerGroup
import com.netflix.spinnaker.clouddriver.model.ServerGroupProvider
import groovy.util.logging.Slf4j

import java.util.concurrent.TimeUnit

@Slf4j
class AmazonClusterProvider2ElectricBoogaloo implements ClusterProvider<AmazonCluster>, ServerGroupProvider {

    private static final TypeReference<Map<String, Object>> JSON = new TypeReference<Map<String, Object>>() {}
    private static final TypeReference<List<Map<String, Object>>> JSON_LIST = new TypeReference<List<Map<String, Object>>>() { }
    private static final ObjectMapper mapper = AmazonObjectMapperConfigurer.createConfigured()
    RegionalCache regionalCache

    AmazonClusterProvider2ElectricBoogaloo(RegionalCache regionalCache) {
        this.regionalCache = regionalCache
    }

    @Override
    Map<String, Set<AmazonCluster>> getClusters() {
        throw new UnsupportedOperationException("this is stupid, no one uses this")
    }

    @Override
    Map<String, Set<AmazonCluster>> getClusterSummaries(String application) {
        return getClusterDetails(application)
    }

    @Override
    Map<String, Set<AmazonCluster>> getClusterDetails(String application) {
        return regionalCache.getAll().keySet().collectEntries { String account -> [(account): getClusters(application, account)] }
    }

    @Override
    Set<AmazonCluster> getClusters(String application, String account) {
        Map<String, AccountRegionData> data = regionalCache.getForAccount(account)
        Set<String> clusterNames = []
        for (Map.Entry<String, AccountRegionData> regionData : data.entrySet()) {
            clusterNames.addAll(regionData.value.asg.getClustersByApp(application))
        }

        return clusterNames.collect { getCluster(application, account, it, true) }
    }

    @Override
    AmazonCluster getCluster(String application, String account, String name) {
        return getCluster(application, account, name, false)
    }

    @Override
    AmazonCluster getCluster(String application, String account, String clusterName, boolean includeDetails) {
        Map<String, AccountRegionData> data = regionalCache.getForAccount(account)

        AmazonCluster cluster = new AmazonCluster(name: clusterName, accountName: account)

        for (Map.Entry<String, AccountRegionData> regionData : data.entrySet()) {
            Map<String, AmazonTargetGroup> atg = new HashMap<>()
            Map<String, AmazonLoadBalancer> lbs = new HashMap<>()
            for (String serverGroupName : regionData.value.asg.getAsgsByCluster(clusterName)) {
                AmazonServerGroup serverGroup = (AmazonServerGroup) getServerGroup(account, regionData.key, serverGroupName, includeDetails)
                if (serverGroup) {
                    cluster.serverGroups.add(serverGroup)
                    serverGroup.targetGroups.each { String name ->
                        TargetGroup targetGroup = regionData.value.tg.getTargetGroupByName(name)
                        if (targetGroup) {
                            AmazonTargetGroup tg = atg.computeIfAbsent(name) { String tgName ->
                                def tg = new AmazonTargetGroup(account: account, region: regionData.key, name: tgName, vpcId: targetGroup.vpcId)
                                for (String lbArn : targetGroup.loadBalancerArns) {
                                    String lbKey = ArnUtils.extractLoadBalancerType(lbArn) + ':' + ArnUtils.extractLoadBalancerName(lbArn)
                                    lbs.computeIfAbsent(lbKey) {
                                        new AmazonLoadBalancer(account: account, region: regionData.key, name: ArnUtils.extractLoadBalancerName(lbArn), vpcId:  tg.vpcId)
                                    }.targetGroups.add(tg)
                                }
                                return tg
                            }
                            Set<String> detatchedInstances = []
                            Set<LoadBalancerInstance> lbis = serverGroup.instances.findResults { InstanceModel inst ->
                                InstanceTargetGroupState itgs = regionData.value.tgh.getByInstanceId(inst.name)?.targetGroups?.find { it.targetGroupName == targetGroup.targetGroupName }
                                if (itgs) {
                                    Map<String, Object> health =
                                            mapper.convertValue(InstanceTargetGroups.fromInstanceTargetGroupStates([itgs])[0], JSON)
                                    return new LoadBalancerInstance(itgs.instanceId, inst.zone, health)
                                } else {
                                    detatchedInstances.add(inst.name)
                                    return null
                                }
                            }
                            tg.serverGroups.add(

                                    new LoadBalancerServerGroup.LoadBalancerServerGroupBuilder()
                                            .name(serverGroup.name)
                                            .account(account)
                                            .region(regionData.key)
                                            .isDisabled(serverGroup.disabled)
                                            .instances(lbis)
                                            .detachedInstances(detatchedInstances)
                                            .build())
                        }
                    }
                    serverGroup.loadBalancers.each { String name ->
                        LoadBalancerDescription lbd = regionData.value.clb.getLoadBalancerByName(name)
                        if (lbd) {
                            AmazonLoadBalancer lb = lbs.computeIfAbsent('classic:' + name) {
                                new AmazonLoadBalancer(account: account, region: regionData.key, name: name, vpcId: lbd.getVPCId())
                            }
                            Set<String> detatchedInstances = []
                            Set<LoadBalancerInstance> lbis = serverGroup.instances.findResults { InstanceModel inst ->
                                InstanceLoadBalancerState ilbs = regionData.value.clbis.getByInstanceId(inst.name)?.loadBalancers?.find { it.loadBalancerName == lbd.loadBalancerName }
                                if (ilbs) {
                                    Map<String, Object> health = mapper.convertValue(InstanceLoadBalancers.fromInstanceLoadBalancerStates([ilbs])[0], JSON)
                                    return new LoadBalancerInstance(ilbs.instanceId, inst.zone, health)
                                } else {
                                    detatchedInstances.add(inst.name)
                                    return null
                                }
                            }
                            lb.serverGroups.add(
                                    new LoadBalancerServerGroup.LoadBalancerServerGroupBuilder()
                                            .name(serverGroup.name)
                                            .account(account)
                                            .region(regionData.key)
                                            .isDisabled(serverGroup.disabled)
                                            .instances(lbis)
                                            .detachedInstances(detatchedInstances)
                                            .build())

                        }

                    }
                }
            }
            cluster.targetGroups.addAll(atg.values())
            cluster.loadBalancers.addAll(lbs.values())
        }

        return cluster
    }

    private static <T> T timeIt(String name, Closure<T> closure) {
        log.info("TIMER::starting $name")
        long startTime = System.nanoTime()
        T result
        try {
            result = closure.call()
        } catch (Throwable t) {
            log.info("TIMER::failure $name after ${TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime)}µs", t)
            throw t
        }
        log.info("TIMER::completed $name in ${TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime)}µs")
        return result
    }

    @Override
    ServerGroup getServerGroup(String account, String region, String name, boolean includeDetails) {
        AccountRegionData data = regionalCache.getForAccountAndRegion(account, region)
        if (!data) {
            log.info("No AccountRegionData for $account/$region")
            return null
        }

        AutoScalingGroup asg = timeIt('asg', {data.asg.getAsgByName(name)})
        List<Subnet> subnets = timeIt('subnets', {data.subnets.getByVPCZoneIdentifier(asg.VPCZoneIdentifier)})
        List<ScalingPolicy> scalingPolicies = timeIt('scalingPolicies', { data.sp.getScalingPolicyByAsgName(asg.autoScalingGroupName)})
        List<ScheduledUpdateGroupAction> scheduledActions = timeIt('scheduledActions', { data.sa.getScheduledActionByAsgName(asg.autoScalingGroupName)})
        Set<String> alarmArns = scalingPolicies.findResults { it.alarms*.alarmARN }.flatten()
        List<MetricAlarm> alarms = timeIt('alarms', {data.metric.getAlarmsByArns(alarmArns)})

        LaunchConfiguration lc = timeIt('launchConfig', { data.lc.getLaunchConfigurationByName(asg.launchConfigurationName)})
        Image image = lc ? timeIt('image', { data.img.getByImageId(lc.imageId)}) : null

        List<Instance> instances = timeIt('instances', { data.inst.getByASGName(asg.autoScalingGroupName)})
        List<EurekaInstance> eureka = timeIt('eureka', { data.eureka.getInstancesInAccountByInstanceId(data.account.accountId, instances*.instanceId)})

        return timeIt('assembleResult', {
            AmazonServerGroup sg = new AmazonServerGroup()

            sg.name = asg.autoScalingGroupName
            sg.region = region
            sg.zones = subnets*.availabilityZone as Set<String>
            sg.vpcId = (subnets*.vpcId as Set<String>)[0]
            sg.instances = instances.findResults { Instance i ->
                    return timeIt('instanceTransform', {
                        AmazonInstance inst = mapper.convertValue(i, AmazonInstance)
                        inst.name = i.instanceId
                        inst.health = [
                                InstanceCachingAgent.getAmazonHealth(i),
                                eureka.findResult {
                                    it.instanceId == i.instanceId ? mapper.convertValue(it, JSON) : null
                                },
                                [ data.clbis.getByInstanceId(i.instanceId) ].findResults { it ? mapper.convertValue(it, JSON) : null}[0],
                                [ data.tgh.getByInstanceId(i.instanceId) ].findResults { it ? mapper.convertValue(it, JSON) : null}[0]
                        ].findResults { it }
                        return inst
                    })
            }
            if (image) {
                sg.image = mapper.convertValue(image, JSON)
                sg.buildInfo = AmazonClusterProvider.buildBuildInfoFromImage(sg.image.tags as List<Map>, 'http://builds.netflix.com')
            }
            sg.set('targetGroups', asg.targetGroupARNs.findResults { ArnUtils.extractTargetGroupName(it) })
            if (lc) {
                sg.launchConfig = mapper.convertValue(lc, JSON)
                sg.set('launchConfigName', lc.launchConfigurationName)
            }
            sg.asg = mapper.convertValue(asg, JSON)
            sg.scheduledActions = mapper.convertValue(scheduledActions, JSON_LIST)
            sg.scalingPolicies = scalingPolicies.findResults { ScalingPolicy sp ->
                def json = mapper.convertValue(sp, JSON)
                Collection<String> policyAlarms = sp.alarms*.alarmARN
                json.alarms = mapper.convertValue(alarms.findAll { policyAlarms.contains(it.alarmArn) }, JSON_LIST)
                return json
            }
            return sg
        })
    }

    @Override
    ServerGroup getServerGroup(String account, String region, String name) {
        return getServerGroup(account, region, name, false)
    }

    @Override
    String getCloudProviderId() {
        return AmazonCloudProvider.ID
    }

    @Override
    boolean supportsMinimalClusters() {
        return false
    }

    @Override
    Collection<String> getServerGroupIdentifiers(String account, String region) {
        return null
    }

    @Override
    String buildServerGroupIdentifier(String account, String region, String serverGroupName) {
        return null
    }
}
