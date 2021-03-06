/*
 * Copyright 2014 Netflix, Inc.
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

package com.netflix.spinnaker.clouddriver.aws.deploy.ops;

import com.netflix.spinnaker.clouddriver.aws.deploy.description.AbstractRegionAsgInstanceIdsDescription;
import com.netflix.spinnaker.clouddriver.aws.deploy.description.InstanceLoadBalancerRegistrationDescription;

public class RegisterInstancesWithLoadBalancerAtomicOperation extends AbstractInstanceLoadBalancerRegistrationAtomicOperation {
  public RegisterInstancesWithLoadBalancerAtomicOperation(AbstractRegionAsgInstanceIdsDescription description) {
    super((InstanceLoadBalancerRegistrationDescription) description);
  }

  @Override
  public RegistrationAction getRegistrationAction() {
    return RegistrationAction.REGISTER;
  }

  @Override
  public String getPhaseName() {
    return "REGISTER_INSTANCES_WITH_LB";
  }
}
