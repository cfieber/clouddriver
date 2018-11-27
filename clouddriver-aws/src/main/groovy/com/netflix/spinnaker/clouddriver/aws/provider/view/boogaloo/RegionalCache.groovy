package com.netflix.spinnaker.clouddriver.aws.provider.view.boogaloo

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
import com.amazonaws.services.elasticloadbalancingv2.model.TargetHealthDescription
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.hash.Hashing
import com.netflix.awsobjectmapper.AmazonObjectMapperConfigurer
import com.netflix.frigga.Names
import com.netflix.spinnaker.clouddriver.aws.data.ArnUtils
import com.netflix.spinnaker.clouddriver.aws.model.InstanceTargetGroupState
import com.netflix.spinnaker.clouddriver.aws.model.InstanceTargetGroups
import com.netflix.spinnaker.clouddriver.aws.model.edda.InstanceLoadBalancers
import com.netflix.spinnaker.clouddriver.aws.model.edda.LoadBalancerInstanceState
import com.netflix.spinnaker.clouddriver.aws.model.edda.TargetGroupHealth
import com.netflix.spinnaker.clouddriver.aws.security.AmazonCredentials
import com.netflix.spinnaker.clouddriver.aws.security.NetflixAmazonCredentials
import com.netflix.spinnaker.clouddriver.eureka.model.EurekaInstance
import com.netflix.spinnaker.clouddriver.security.AccountCredentialsProvider
import com.netflix.spinnaker.kork.jedis.RedisClientDelegate
import groovy.util.logging.Slf4j
import redis.clients.jedis.BinaryJedisCommands

import java.nio.charset.Charset
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Function
import java.util.zip.GZIPInputStream

@Slf4j
class RegionalCache implements Closeable {

    //environment -> region -> data
    Map<String, Map<String, EnvironmentRegionData>> environmentData = [:]

    //account -> region -> data
    Map<String, Map<String, AccountRegionData>> accountData = [:]

    //region -> data
    Map<String, ImageCache> images = [:]

    AccountRegionData getForAccountAndRegion(String account, String region) {
        return accountData.get(account)?.get(region)
    }

    Map<String, Map<String, AccountRegionData>> getAll() {
        return accountData
    }

    Map<String, AccountRegionData> getForAccount(String account) {
        return accountData.get(account)
    }

    private Encachenator encachenator
    private ScheduledExecutorService sched

    @Override
    public void close() {
        sched.shutdown()
        if (encachenator != null) {
            encachenator.close()
        }

        if (!sched.awaitTermination(5, TimeUnit.SECONDS)) {
            sched.shutdownNow()
        }
    }

    public RegionalCache(AccountCredentialsProvider acp, RedisClientDelegate redisClientDelegate, boolean encachenate) {
        if (encachenate) {
            log.info("creating encachenator")
            encachenator = new Encachenator(acp, redisClientDelegate)
        }
        sched = Executors.newScheduledThreadPool(10)

        NetflixAmazonCredentials bake = acp.getCredentials('test')
        bake.regions.each { AmazonCredentials.AWSRegion bakeRegion ->
            log.info("Scheduling image cache for $bakeRegion.name")
            images[bakeRegion.name] = new ImageCache(bakeRegion.name, bake, redisClientDelegate)
            long delayJitter = (Math.random() * 30000d)
            sched.scheduleWithFixedDelay(images[bakeRegion.name].buildRefreshJob(), delayJitter, 5000, TimeUnit.MILLISECONDS)
        }
        def amznCreds = acp.all.findAll { it instanceof NetflixAmazonCredentials && it.eddaEnabled }
        amznCreds.each { NetflixAmazonCredentials cred ->
            Map<String, EnvironmentRegionData> env = environmentData.computeIfAbsent(cred.getEnvironment()) { [:] }
            Map<String, AccountRegionData> ards = cred.regions.collectEntries { AmazonCredentials.AWSRegion region ->
                EnvironmentRegionData erd = env.computeIfAbsent(region.name) {
                    new EnvironmentRegionData(cred.getEnvironment(), region.name)
                }
                log.info("Creating AccountRegionData for $cred.name/$region.name")
                AccountRegionData ard = new AccountRegionData(cred, region.name, erd, images[region.name], redisClientDelegate, sched)
                [(region.name): ard]
            }
            accountData.put(cred.name, ards)
        }
    }
}


class EnvironmentRegionData {
    String environment
    String region

    EurekaCache eureka

    EnvironmentRegionData(String environment, String region) {
        this.environment = environment
        this.region = region
        this.eureka = new EurekaCache(region, environment)
    }
}

@Slf4j
class AmazonRegionData<T> {

    @Slf4j
    static class RefreshJob<T> implements Runnable {
        private static final ObjectMapper mapper = AmazonObjectMapperConfigurer.createConfigured()
        final AmazonRegionData<T> regionData

        RefreshJob(AmazonRegionData<T> regionData) {
            this.regionData = regionData
        }

        void run() {
            try {
                log.info("$regionData.key starting")
                long start = System.nanoTime()
                boolean hashMatch = false
                byte[] eddaData = timeIt("$regionData.key fetch redis data") {
                    return regionData.redisClientDelegate.withBinaryClient({ bc ->
                        def currentHash = regionData.currentState.get().dataHash
                        if (currentHash != null) {
                            def redisHash = bc.get(regionData.redisHashKey)
                            hashMatch = Arrays.equals(redisHash, currentHash)
                        }
                        if (!hashMatch) {
                            return bc.get(regionData.redisKey)
                        }
                        return null
                    } as Function<BinaryJedisCommands, byte[]>)
                }

                if (hashMatch) {
                    log.info("$regionData.key hash matches, skipping refresh")
                    return
                }


                List<T> cacheData = []
                byte[] hash = null
                if (eddaData != null && eddaData.length > 0) {
                    hash = Hashing.murmur3_128().hashBytes(eddaData).asBytes()
                    eddaData = timeIt("$regionData.key inflate redis data") {
                        return new GZIPInputStream(new ByteArrayInputStream(eddaData)).bytes
                    }

                    cacheData = timeIt("$regionData.key object mappering") {
                        return (List<T>) mapper.readValue(eddaData, regionData.typeRef)
                    }
                }

                Map<String, Map> indexes = timeIt("$regionData.key indexing") { regionData.buildIndexes(cacheData) }
                log.info("$regionData.key total rebuild time from redis data set ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)}millis")

                regionData.currentState.set(new State<>(cacheData, indexes, hash))
            } catch (e) {
                log.warn("Failboat", e)
            }

        }
        private static <T> T timeIt(GString msg, Closure<T> closure) {
            return timeIt(msg.toString(), closure)
        }
        private static <T> T timeIt(String msg, Closure<T> closure) {
            long startTime = System.nanoTime()
            try {
                T rv = closure.call()
                log.info("$msg completed in ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)}millis")
                return rv
            } catch (e) {
                log.info("$msg failed in ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)}millis")
                throw e
            }
        }
    }

    String region
    NetflixAmazonCredentials creds
    String eddaCollection
    TypeReference<List<T>> typeRef
    RedisClientDelegate redisClientDelegate

    static class State<T> {
        final List<T> cacheData
        final Map<String, Map> indexes
        final byte[] dataHash

        State(List<T> cacheData, Map<String, Map> indexes, dataHash) {
            this.cacheData = cacheData
            this.indexes = indexes
            this.dataHash = dataHash
        }
    }

    final AtomicReference<State<T>> currentState = new AtomicReference<>(new State<>(Collections.emptyList(), Collections.emptyMap(), null))
    String key
    byte[] redisKey
    byte[] redisHashKey

    AmazonRegionData(String region, NetflixAmazonCredentials creds, String eddaCollection, TypeReference<List<T>> typeRef, RedisClientDelegate redisClientDelegate) {
        this.region = region
        this.creds = creds
        this.eddaCollection = eddaCollection
        this.typeRef = typeRef
        this.redisClientDelegate = redisClientDelegate
        key = "$creds.name/$region/$eddaCollection"
        redisKey = key.getBytes(Charset.forName('US-ASCII'))
        redisHashKey = "HASH:$key".getBytes(Charset.forName('US-ASCII'))
    }

    RefreshJob<T> buildRefreshJob() {
        return new RefreshJob<>(this)
    }

    protected Map<String, Map> buildIndexes(List<T> cacheData) {
    }
}

class AccountRegionData {
    NetflixAmazonCredentials account
    String region
    EnvironmentRegionData environmentRegionData
    ImageCache img

    ASGCache asg
    MetricAlarmCache metric
    ScalingPolicyCache sp
    ScheduledActionsCache sa
    LaunchConfigCache lc
    TargetGroupCache tg
    TargetGroupHealthCache tgh
    ClassicLoadBalancerInstanceStateCache clbis
    ClassicLoadBalancerCache clb
    InstanceCache inst
    SubnetCache subnets
    ScheduledExecutorService sched

    AccountRegionData(NetflixAmazonCredentials account, String region, EnvironmentRegionData environmentRegionData, ImageCache imageCache, RedisClientDelegate redisClientDelegate, ScheduledExecutorService sched) {
        this.region = region
        this.account = account
        this.environmentRegionData = environmentRegionData
        this.img = imageCache
        this.sched = sched
        asg = mkData { new ASGCache(region, account, redisClientDelegate) }
        metric = mkData { new MetricAlarmCache(region, account, redisClientDelegate) }
        sp = mkData { new ScalingPolicyCache(region, account, redisClientDelegate) }
        sa = mkData { new ScheduledActionsCache(region, account, redisClientDelegate) }
        lc = mkData { new LaunchConfigCache(region, account, redisClientDelegate) }
        tg = mkData { new TargetGroupCache(region, account, redisClientDelegate) }
        tgh = mkData { new TargetGroupHealthCache(region, account, redisClientDelegate) }
        clbis = mkData { new ClassicLoadBalancerInstanceStateCache(region, account, redisClientDelegate) }
        clb = mkData { new ClassicLoadBalancerCache(region, account, redisClientDelegate) }
        inst = mkData { new InstanceCache(region, account, redisClientDelegate) }
        subnets = mkData { new SubnetCache(region, account, redisClientDelegate) }
    }

    private <T extends AmazonRegionData<?>> T mkData(Closure <T> closure) {
        T result = closure.call()
        long jitter = (Math.random() * 30000d)
        sched.scheduleWithFixedDelay(result.buildRefreshJob(), jitter, 5000, TimeUnit.MILLISECONDS)
        return result
    }

    EurekaCache getEureka() {
        return environmentRegionData.eureka
    }
}

class SubnetCache extends AmazonRegionData<Subnet> {

    SubnetCache(String region, NetflixAmazonCredentials creds, RedisClientDelegate redisClientDelegate) {
        super(region, creds, 'aws/subnets', new TypeReference<List<Subnet>>() {}, redisClientDelegate)
    }

    List<Subnet> getByVPCZoneIdentifier(String vpcZoneIdentifier) {
        Collection<String> subnetIds = vpcZoneIdentifier.split(',')
        currentState.get().cacheData.findAll { subnetIds.contains(it.subnetId) }
    }
}

class EurekaCache {
    String region
    String environment
    List<EurekaInstance> instances

    EurekaCache(String region, String environment) {
        this.region = region
        this.environment = environment
        instances = []
    }

    List<EurekaInstance> getInstancesInAccountByInstanceId(String accountId, Collection<String> instanceIds) {
        instances.findAll { it.accountId == accountId && instanceIds.contains(it.instanceId) }
    }
}

class InstanceCache extends AmazonRegionData<Instance> {

    InstanceCache(String region, NetflixAmazonCredentials creds, RedisClientDelegate redisClientDelegate) {
        super(region, creds, 'view/instances', new TypeReference<List<Instance>>() {}, redisClientDelegate)
    }

    @Override
    protected Map<String, Map> buildIndexes(List<Instance> cacheData) {
        def indexes = [:]
        indexes.byASGName = new HashMap<>()
        for (Instance instance : cacheData) {
            String asgName = instance.tags.find { it.key == 'aws:autoscaling:groupName' }?.value
            if (asgName) {
                indexes.byASGName.computeIfAbsent(asgName, { [] }).add(instance)
            }
        }
        return indexes
    }

    List<Instance> getByASGName(String asgName) {
        return currentState.get().indexes.byASGName?.get(asgName) ?: []
    }
}
class MetricAlarmCache extends AmazonRegionData<MetricAlarm> {

    MetricAlarmCache(String region, NetflixAmazonCredentials creds, RedisClientDelegate redisClientDelegate) {
        super(region, creds, 'aws/alarms', new TypeReference<List<MetricAlarm>>() {}, redisClientDelegate)
    }

    @Override
    protected Map<String, Map> buildIndexes(List<MetricAlarm> cacheData) {
        def indexes = [:]
        indexes.byArn = new HashMap<String, MetricAlarm>()
        for (MetricAlarm alarm : cacheData) {
            indexes.byArn.put(alarm.alarmArn, alarm)
        }
        return indexes
    }

    List<MetricAlarm> getAlarmsByArns(Collection<String> arns) {
        Map<String, MetricAlarm> byArn = currentState.get().indexes.byArn ?: [:]
        return arns.findResults { byArn.get(it) }
    }
}

class ScheduledActionsCache extends AmazonRegionData<ScheduledUpdateGroupAction> {

    ScheduledActionsCache(String region, NetflixAmazonCredentials creds, RedisClientDelegate redisClientDelegate) {
        super(region, creds, 'aws/scheduledActions', new TypeReference<List<ScheduledUpdateGroupAction>>() {}, redisClientDelegate)
    }

    @Override
    protected Map<String, Map> buildIndexes(List<ScheduledUpdateGroupAction> cacheData) {
        def indexes = [:]
        indexes.byAsg = new HashMap<String, ScheduledUpdateGroupAction>()
        for (ScheduledUpdateGroupAction action : cacheData) {
            indexes.byAsg.computeIfAbsent(action.autoScalingGroupName, { [] }).add(action)
        }
        return indexes
    }

    List<ScheduledUpdateGroupAction> getScheduledActionByAsgName(String name) {
        return currentState.get().indexes.byAsg?.get(name) ?: []
    }
}

class ScalingPolicyCache extends AmazonRegionData<ScalingPolicy> {

    ScalingPolicyCache(String region, NetflixAmazonCredentials creds, RedisClientDelegate redisClientDelegate) {
        super(region, creds, 'aws/scalingPolicies', new TypeReference<List<ScalingPolicy>>() {}, redisClientDelegate)
    }

    @Override
    protected Map<String, Map> buildIndexes(List<ScalingPolicy> cacheData) {
        def indexes = [:]
        indexes.byAsg = new HashMap<>()
        for (ScalingPolicy sp : cacheData) {
            indexes.byAsg.computeIfAbsent(sp.autoScalingGroupName, { [] }).add(sp)
        }
        return indexes
    }

    List<ScalingPolicy> getScalingPolicyByAsgName(String name) {
        return currentState.get().indexes.byAsg?.get(name) ?: []
    }

}
class ASGCache extends AmazonRegionData<AutoScalingGroup> {

    ASGCache(String region, NetflixAmazonCredentials creds, RedisClientDelegate redisClientDelegate) {
        super(region, creds, 'aws/autoScalingGroups', new TypeReference<List<AutoScalingGroup>>() {}, redisClientDelegate)
    }

    @Override
    protected Map<String, Map> buildIndexes(List<AutoScalingGroup> cacheData) {
        def indexes = [:]
        indexes.byName = new HashMap<String, AutoScalingGroup>()
        indexes.asgsByCluster = new HashMap<String, Set<String>>()
        indexes.clustersByApp = new HashMap<String, Set<String>>()
        for (AutoScalingGroup asg : cacheData) {
            indexes.byName.put(asg.autoScalingGroupName, asg)
            def name = Names.parseName(asg.autoScalingGroupName)
            def clusterName = name?.cluster
            if (clusterName) {
                indexes.asgsByCluster.computeIfAbsent(clusterName, { new HashSet<String>() }).add(asg.autoScalingGroupName)
                indexes.clustersByApp.computeIfAbsent(name.app, { new HashSet<String>() }).add(clusterName)
            }
        }
        return indexes
    }

    AutoScalingGroup getAsgByName(String name) {
        return currentState.get().indexes.byName?.get(name)
    }

    Collection<String> getAsgsByCluster(String cluster) {
        return currentState.get().indexes.asgsByCluster?.get(cluster) ?: []
    }

    Collection<String> getClustersByApp(String app) {
        return currentState.get().indexes.clustersByApp?.get(app) ?: []
    }
}

class LaunchConfigCache extends AmazonRegionData<LaunchConfiguration> {

    LaunchConfigCache(String region, NetflixAmazonCredentials creds, RedisClientDelegate redisClientDelegate) {
        super(region, creds, 'aws/launchConfigurations', new TypeReference<List<LaunchConfiguration>>() {}, redisClientDelegate)
    }

    @Override
    protected Map<String, Map> buildIndexes(List<LaunchConfiguration> cacheData) {
        def indexes = [:]
        indexes.byName = new HashMap<String, LaunchConfiguration>()
        for (LaunchConfiguration launchConfiguration : cacheData) {
            indexes.byName.put(launchConfiguration.launchConfigurationName, launchConfiguration)
        }
        return indexes
    }

    LaunchConfiguration getLaunchConfigurationByName(String name) {
        return currentState.get().indexes.byName?.get(name)
    }
}

class ImageCache extends AmazonRegionData<Image> {

    ImageCache(String region, NetflixAmazonCredentials creds, RedisClientDelegate redisClientDelegate) {
        super(region, creds, 'aws/images', new TypeReference<List<Image>>() {}, redisClientDelegate)
    }

    @Override
    protected Map<String, Map> buildIndexes(List<Image> cacheData) {
        def indexes = [:]
        indexes.byImageId = new HashMap<String, Image>()
        for (Image img : cacheData) {
            indexes.byImageId.put(img.imageId, img)
        }
        return indexes
    }

    Image getByImageId(String imageId) {
        return currentState.get().indexes.byImageId?.get(imageId)
    }
}

class TargetGroupCache extends AmazonRegionData<TargetGroup> {

    TargetGroupCache(String region, NetflixAmazonCredentials creds, RedisClientDelegate redisClientDelegate) {
        super(region, creds, 'aws/targetGroups', new TypeReference<List<TargetGroup>>() {}, redisClientDelegate)
    }

    @Override
    protected Map<String, Map> buildIndexes(List<TargetGroup> cacheData) {
        def indexes = [:]
        indexes.byName = new HashMap<String, TargetGroup>()
        for (TargetGroup tg : cacheData) {
            indexes.byName.put(tg.targetGroupName, tg)
        }
        return indexes
    }

    TargetGroup getTargetGroupByName(String name) {
        return currentState.get().indexes.byName?.get(name)
    }
}

class TargetGroupHealthCache extends AmazonRegionData<TargetGroupHealth> {

    TargetGroupHealthCache(String region, NetflixAmazonCredentials creds, RedisClientDelegate redisClientDelegate) {
        super(region, creds, 'view/targetGroupHealth', new TypeReference<List<TargetGroupHealth>>() {}, redisClientDelegate)
    }

    @Override
    protected Map<String, Map> buildIndexes(List<TargetGroupHealth> cacheData) {
        def indexes = [:]
        def itgs = InstanceTargetGroups.fromInstanceTargetGroupStates(cacheData.findResults { TargetGroupHealth tgh ->
            tgh.health.findResults { TargetHealthDescription targetHealthDescription ->
                new InstanceTargetGroupState(
                        targetHealthDescription.target.id,
                        ArnUtils.extractTargetGroupName(tgh.targetGroupArn).get(),
                        targetHealthDescription.targetHealth.state,
                        targetHealthDescription.targetHealth.reason,
                        targetHealthDescription.targetHealth.description)
            }
        }.flatten())
        indexes.byInstanceId = new HashMap<String, InstanceTargetGroups>()
        for (InstanceTargetGroups itg : itgs) {
            indexes.byInstanceId.put(itg.instanceId, itg)
        }
        return indexes
    }

    InstanceTargetGroups getByInstanceId(String instanceId) {
        currentState.get().indexes.byInstanceId?.get(instanceId)
    }
}

class ClassicLoadBalancerCache extends AmazonRegionData<LoadBalancerDescription> {

    ClassicLoadBalancerCache(String region, NetflixAmazonCredentials creds, RedisClientDelegate redisClientDelegate) {
        super(region, creds, 'aws/loadBalancers', new TypeReference<List<LoadBalancerDescription>>() {}, redisClientDelegate)
    }

    @Override
    protected Map<String, Map> buildIndexes(List<LoadBalancerDescription> cacheData) {
        def indexes = [:]
        indexes.byName = new HashMap<String, LoadBalancerDescription>()
        for (LoadBalancerDescription lb : cacheData) {
            indexes.byName.put(lb.loadBalancerName, lb)
        }
        return indexes
    }

    LoadBalancerDescription getLoadBalancerByName(String name) {
        return currentState.get().indexes.byName?.get(name)
    }
}

class ClassicLoadBalancerInstanceStateCache extends AmazonRegionData<LoadBalancerInstanceState> {

    ClassicLoadBalancerInstanceStateCache(String region, NetflixAmazonCredentials creds, RedisClientDelegate redisClientDelegate) {
        super(region, creds, 'view/loadBalancerInstances', new TypeReference<List<LoadBalancerInstanceState>>() {}, redisClientDelegate)
    }

    @Override
    protected Map<String, Map> buildIndexes(List<LoadBalancerInstanceState> cacheData) {
        def indexes = [:]
        def ilbs = InstanceLoadBalancers.fromLoadBalancerInstanceState(cacheData)
        indexes.byInstanceId = new HashMap<String, InstanceLoadBalancers>()
        for (InstanceLoadBalancers ilb : ilbs) {
            indexes.byInstanceId.put(ilb.instanceId, ilb)
        }
        return indexes
    }

    InstanceLoadBalancers getByInstanceId(String instanceId) {
        return currentState.get().indexes.byInstanceId?.get(instanceId)
    }
}
