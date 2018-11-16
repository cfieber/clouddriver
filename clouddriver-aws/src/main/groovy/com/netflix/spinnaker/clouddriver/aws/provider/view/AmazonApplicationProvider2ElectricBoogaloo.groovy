package com.netflix.spinnaker.clouddriver.aws.provider.view

import com.netflix.spinnaker.clouddriver.model.Application
import com.netflix.spinnaker.clouddriver.model.ApplicationProvider

class AmazonApplicationProvider2ElectricBoogaloo implements ApplicationProvider {
    @Override
    Set<? extends Application> getApplications(boolean expand) {
        return []
    }

    @Override
    Application getApplication(String name) {
        return null
    }
}
