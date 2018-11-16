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
import com.netflix.awsobjectmapper.AmazonObjectMapperConfigurer
import com.netflix.frigga.Names
import com.netflix.spinnaker.clouddriver.aws.data.ArnUtils
import com.netflix.spinnaker.clouddriver.aws.model.InstanceTargetGroupState
import com.netflix.spinnaker.clouddriver.aws.model.InstanceTargetGroups
import com.netflix.spinnaker.clouddriver.aws.model.edda.InstanceLoadBalancers
import com.netflix.spinnaker.clouddriver.aws.model.edda.LoadBalancerInstanceState
import com.netflix.spinnaker.clouddriver.aws.model.edda.TargetGroupHealth
import com.netflix.spinnaker.clouddriver.aws.security.AmazonCredentials
import com.netflix.spinnaker.clouddriver.aws.security.EddaTemplater
import com.netflix.spinnaker.clouddriver.aws.security.NetflixAmazonCredentials
import com.netflix.spinnaker.clouddriver.eureka.model.EurekaInstance
import com.netflix.spinnaker.clouddriver.security.AccountCredentialsProvider
import groovy.util.logging.Slf4j
import org.apache.http.impl.client.HttpClientBuilder
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.RequestEntity
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter
import org.springframework.web.client.RestTemplate


class RegionalCache {

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

    public RegionalCache(AccountCredentialsProvider acp) {
        NetflixAmazonCredentials bake = acp.getCredentials('test')
        bake.regions.each { AmazonCredentials.AWSRegion bakeRegion ->
            images[bakeRegion.name] = new ImageCache(bakeRegion.name, bake)
        }
        def amznCreds = acp.all.findAll { it instanceof NetflixAmazonCredentials && it.eddaEnabled }
        amznCreds.each { NetflixAmazonCredentials cred ->
            Map<String, EnvironmentRegionData> env = environmentData.computeIfAbsent(cred.getEnvironment()) { [:] }
            Map<String, AccountRegionData> ards = cred.regions.collectEntries { AmazonCredentials.AWSRegion region ->
                EnvironmentRegionData erd = env.computeIfAbsent(region.name) {
                    new EnvironmentRegionData(cred.getEnvironment(), region.name)
                }
                AccountRegionData ard = new AccountRegionData(cred, region.name, erd, images[region.name])
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

    private static final RestTemplate tmpl = buildRestTemplate()

    static RestTemplate buildRestTemplate() {
        def mapper = AmazonObjectMapperConfigurer.createConfigured()
        HttpComponentsClientHttpRequestFactory clientHttpRequestFactory = new HttpComponentsClientHttpRequestFactory(
                HttpClientBuilder.create().build())
        def tmpl = new RestTemplate(clientHttpRequestFactory)
        tmpl.setMessageConverters([new MappingJackson2HttpMessageConverter(mapper)])
        return tmpl
    }

    String region
    NetflixAmazonCredentials creds

    String eddaCollection

    List<T> cacheData

    AmazonRegionData(String region, NetflixAmazonCredentials creds, String eddaCollection, ParameterizedTypeReference<List<T>> typeRef) {
        this.region = region
        this.creds = creds
        this.eddaCollection = eddaCollection

        URI uri = URI.create(EddaTemplater.defaultTemplater().getUrl(creds.edda, region) + '/REST/v2/aws/' + eddaCollection + ';_expand').normalize()
        long startTime = System.currentTimeMillis()
        log.info("Loading $creds.name/$region/$eddaCollection from ${uri.toASCIIString()}")
        def headers = new HttpHeaders()
        headers.set(HttpHeaders.ACCEPT_ENCODING, 'gzip')
        RequestEntity re = new RequestEntity(headers, HttpMethod.GET, uri)
        cacheData = tmpl.exchange(re, typeRef).getBody()
        log.info("Loading $creds.name/$region/$eddaCollection completed in ${System.currentTimeMillis() - startTime}ms")
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

    AccountRegionData(NetflixAmazonCredentials account, String region, EnvironmentRegionData environmentRegionData, ImageCache imageCache) {
        this.region = region
        this.account = account
        this.environmentRegionData = environmentRegionData
        this.img = imageCache
        asg = new ASGCache(region, account)
        metric = new MetricAlarmCache(region, account)
        sp = new ScalingPolicyCache(region, account)
        sa = new ScheduledActionsCache(region, account)
        lc = new LaunchConfigCache(region, account)
        tg = new TargetGroupCache(region, account)
        tgh = new TargetGroupHealthCache(region, account)
        clbis = new ClassicLoadBalancerInstanceStateCache(region, account)
        clb = new ClassicLoadBalancerCache(region, account)
        inst = new InstanceCache(region, account)
        subnets = new SubnetCache(region, account)
    }

    EurekaCache getEureka() {
        return environmentRegionData.eureka
    }
}

class SubnetCache extends AmazonRegionData<Subnet> {

    SubnetCache(String region, NetflixAmazonCredentials creds) {
        super(region, creds, 'subnets', new ParameterizedTypeReference<List<Subnet>>() {})
    }

    List<Subnet> getByVPCZoneIdentifier(String vpcZoneIdentifier) {
        Collection<String> subnetIds = vpcZoneIdentifier.split(',')
        cacheData.findAll { subnetIds.contains(it.subnetId) }
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

    Map<String, List<Instance>> byASGName

    InstanceCache(String region, NetflixAmazonCredentials creds) {
        super(region, creds, '../view/instances', new ParameterizedTypeReference<List<Instance>>() {})
        byASGName = new HashMap<>()
        for (Instance instance : cacheData) {
            String asgName = instance.tags.find { it.key == 'aws:autoscaling:groupName' }?.value
            if (asgName) {
                byASGName.computeIfAbsent(asgName, { [] }).add(instance)
            }
        }
    }

    List<Instance> getByASGName(String asgName) {
        byASGName.get(asgName) ?: []
    }
}
class MetricAlarmCache extends AmazonRegionData<MetricAlarm> {

    Map<String, MetricAlarm> byArn

    MetricAlarmCache(String region, NetflixAmazonCredentials creds) {
        super(region, creds, 'alarms', new ParameterizedTypeReference<List<MetricAlarm>>() {})
        byArn = new HashMap<>(cacheData.size() + 5000)
        for (MetricAlarm alarm : cacheData) {
            byArn.put(alarm.alarmArn, alarm)
        }
    }

    List<MetricAlarm> getAlarmsByArns(Collection<String> arns) {
        arns.findResults { byArn.get(it) }
    }
}

class ScheduledActionsCache extends AmazonRegionData<ScheduledUpdateGroupAction> {

    Map<String, List<ScheduledUpdateGroupAction>> byAsg

    ScheduledActionsCache(String region, NetflixAmazonCredentials creds) {
        super(region, creds, 'scheduledActions', new ParameterizedTypeReference<List<ScheduledUpdateGroupAction>>() {})
        byAsg = new HashMap<>()
        for (ScheduledUpdateGroupAction action : cacheData) {
            byAsg.computeIfAbsent(action.autoScalingGroupName, { [] }).add(action)
        }
    }

    List<ScheduledUpdateGroupAction> getScheduledActionByAsgName(String name) {
        byAsg.get(name) ?: []
    }
}

class ScalingPolicyCache extends AmazonRegionData<ScalingPolicy> {

    Map<String, List<ScalingPolicy>> byAsg

    ScalingPolicyCache(String region, NetflixAmazonCredentials creds) {
        super(region, creds, 'scalingPolicies', new ParameterizedTypeReference<List<ScalingPolicy>>() {})
        byAsg = new HashMap<>()
        for (ScalingPolicy sp : cacheData) {
            byAsg.computeIfAbsent(sp.autoScalingGroupName, { [] }).add(sp)
        }
    }

    List<ScalingPolicy> getScalingPolicyByAsgName(String name) {
        byAsg.get(name) ?: []
    }

}
class ASGCache extends AmazonRegionData<AutoScalingGroup> {

    Map<String, AutoScalingGroup> byName

    Map<String, Set<String>> asgsByCluster

    Map<String, Set<String>> clustersByApp

    ASGCache(String region, NetflixAmazonCredentials creds) {
        super(region, creds, 'autoScalingGroups', new ParameterizedTypeReference<List<AutoScalingGroup>>() {})
        byName = new HashMap<>(cacheData.size() + 5000)
        asgsByCluster = new HashMap<>()
        clustersByApp = new HashMap<>()
        for (AutoScalingGroup asg : cacheData) {
            byName.put(asg.autoScalingGroupName, asg)
            def name = Names.parseName(asg.autoScalingGroupName)
            def clusterName = name?.cluster
            if (clusterName) {
                asgsByCluster.computeIfAbsent(clusterName, { new HashSet<>() }).add(asg.autoScalingGroupName)
                clustersByApp.computeIfAbsent(name.app, { new HashSet<>() }).add(clusterName)
            }
        }
    }

    AutoScalingGroup getAsgByName(String name) {
        byName.get(name)
    }

    Collection<String> getAsgsByCluster(String cluster) {
        asgsByCluster.get(cluster) ?: []
    }

    Collection<String> getClustersByApp(String app) {
        clustersByApp.get(app) ?: []
    }
}

class LaunchConfigCache extends AmazonRegionData<LaunchConfiguration> {

    Map<String, LaunchConfiguration> byName

    LaunchConfigCache(String region, NetflixAmazonCredentials creds) {
        super(region, creds, 'launchConfigurations', new ParameterizedTypeReference<List<LaunchConfiguration>>() {})
        byName = new HashMap<>(cacheData.size() + 5000)
        for (LaunchConfiguration launchConfiguration : cacheData) {
            byName.put(launchConfiguration.launchConfigurationName, launchConfiguration)
        }
    }

    LaunchConfiguration getLaunchConfigurationByName(String name) {
        byName.get(name)
    }
}

class ImageCache extends AmazonRegionData<Image> {

    Map<String, Image> byImageId
    ImageCache(String region, NetflixAmazonCredentials creds) {
        super(region, creds, 'images', new ParameterizedTypeReference<List<Image>>() {})
        byImageId = new HashMap<>(cacheData.size() + 5000)
        for (Image img : cacheData) {
            byImageId.put(img.imageId, img)
        }
    }

    Image getByImageId(String imageId) {
        byImageId.get(imageId)
    }
}

class TargetGroupCache extends AmazonRegionData<TargetGroup> {

    Map<String, TargetGroup> byName

    TargetGroupCache(String region, NetflixAmazonCredentials creds) {
        super(region, creds, 'targetGroups', new ParameterizedTypeReference<List<TargetGroup>>() {})
        byName = new HashMap<>()
        for (TargetGroup tg : cacheData) {
            byName.put(tg.targetGroupName, tg)
        }
    }

    TargetGroup getTargetGroupByName(String name) {
        return byName.get(name)
    }
}

class TargetGroupHealthCache extends AmazonRegionData<TargetGroupHealth> {

    Map<String, InstanceTargetGroups> byInstanceId

    TargetGroupHealthCache(String region, NetflixAmazonCredentials creds) {
        super(region, creds, '../view/targetGroupHealth', new ParameterizedTypeReference<List<TargetGroupHealth>>() {})
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
        byInstanceId = new HashMap<>(itgs.size() + 5000)
        for (InstanceTargetGroups itg : itgs) {
            byInstanceId.put(itg.instanceId, itg)
        }
    }

    InstanceTargetGroups getByInstanceId(String instanceId) {
        byInstanceId.get(instanceId)
    }
}

class ClassicLoadBalancerCache extends AmazonRegionData<LoadBalancerDescription> {

    Map<String, LoadBalancerDescription> byName

    ClassicLoadBalancerCache(String region, NetflixAmazonCredentials creds) {
        super(region, creds, 'loadBalancers', new ParameterizedTypeReference<List<LoadBalancerDescription>>() {})
        byName = new HashMap<>(cacheData.size() + 5000)
        for (LoadBalancerDescription lb : cacheData) {
            byName.put(lb.loadBalancerName, lb)
        }
    }

    LoadBalancerDescription getLoadBalancerByName(String name) {
        return byName.get(name)
    }
}

class ClassicLoadBalancerInstanceStateCache extends AmazonRegionData<LoadBalancerInstanceState> {

    Map<String, InstanceLoadBalancers> byInstanceId

    ClassicLoadBalancerInstanceStateCache(String region, NetflixAmazonCredentials creds) {
        super(region, creds, '../view/loadBalancerInstances', new ParameterizedTypeReference<List<LoadBalancerInstanceState>>() {})
        def ilbs = InstanceLoadBalancers.fromLoadBalancerInstanceState(cacheData)
        byInstanceId = new HashMap<>(ilbs.size() + 5000)
        for (InstanceLoadBalancers ilb : ilbs) {
            byInstanceId.put(ilb.instanceId, ilb)
        }
    }

    InstanceLoadBalancers getByInstanceId(String instanceId) {
        byInstanceId.get(instanceId)
    }
}
