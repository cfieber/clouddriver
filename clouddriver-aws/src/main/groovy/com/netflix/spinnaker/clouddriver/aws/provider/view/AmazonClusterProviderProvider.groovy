package com.netflix.spinnaker.clouddriver.aws.provider.view

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.cats.cache.Cache
import com.netflix.spinnaker.clouddriver.aws.provider.AwsProvider
import com.netflix.spinnaker.clouddriver.aws.provider.view.boogaloo.RegionalCache
import com.netflix.spinnaker.clouddriver.security.AccountCredentialsProvider
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class AmazonClusterProviderProvider {
    @Bean
    @ConditionalOnProperty(value = 'amazonProviders.original', matchIfMissing = true)
    AmazonClusterProvider amazonClusterProvider(Cache cacheView, AwsProvider awsProvider) {
        return new AmazonClusterProvider(cacheView, awsProvider)
    }


    @Bean
    @ConditionalOnProperty(value = 'amazonProviders.original', matchIfMissing = true)
    AmazonApplicationProvider amazonApplicationProvider(Cache cacheView, ObjectMapper objectMapper) {
        return new AmazonApplicationProvider(cacheView, objectMapper)
    }

    @Bean
    @ConditionalOnProperty(value = 'amazonProviders.original', havingValue = 'false')
    AmazonApplicationProvider2ElectricBoogaloo amazonApplicationProvider2ElectricBoogaloo() {
        return new AmazonApplicationProvider2ElectricBoogaloo()
    }

    @Bean
    @ConditionalOnProperty(value = 'amazonProviders.original', havingValue = 'false')
    RegionalCache regionalCache(AccountCredentialsProvider accountCredentialsProvider) {
        new RegionalCache(accountCredentialsProvider)
    }

    @Bean
    @ConditionalOnProperty(value = 'amazonProviders.original', havingValue = 'false')
    AmazonClusterProvider2ElectricBoogaloo amazonClusterProvider2ElectricBoogaloo(RegionalCache regionalCache) {
        return new AmazonClusterProvider2ElectricBoogaloo(regionalCache)
    }
}
