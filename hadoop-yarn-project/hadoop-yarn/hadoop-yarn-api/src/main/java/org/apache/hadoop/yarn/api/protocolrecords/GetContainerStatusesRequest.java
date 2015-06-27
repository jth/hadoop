/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * The request sent by the <code>ApplicationMaster</code> to the
 * <code>NodeManager</code> to get {@link ContainerStatus} of requested
 * containers.
 * 
 * @see ContainerManagementProtocol#getContainerStatuses(GetContainerStatusesRequest)
 */
@Public
@Stable
public abstract class GetContainerStatusesRequest {

  @Public
  @Stable
  public static GetContainerStatusesRequest newInstance(
          List<ContainerId> containerIds, Resource capability) {
    GetContainerStatusesRequest request =
            Records.newRecord(GetContainerStatusesRequest.class);
    request.setContainerIds(containerIds);
    request.setCapability(capability);
    return request;
  }

  @Public
  @Stable
  public static GetContainerStatusesRequest newInstance(
      List<ContainerId> containerIds) {
    GetContainerStatusesRequest request =
        Records.newRecord(GetContainerStatusesRequest.class);
    request.setContainerIds(containerIds);
    return request;
  }

  /**
   * Get the list of <code>ContainerId</code>s of containers for which to obtain
   * the <code>ContainerStatus</code>.
   * 
   * @return the list of <code>ContainerId</code>s of containers for which to
   *         obtain the <code>ContainerStatus</code>.
   */
  @Public
  @Stable
  public abstract List<ContainerId> getContainerIds();

  /**
   * Set a list of <code>ContainerId</code>s of containers for which to obtain
   * the <code>ContainerStatus</code>
   * 
   * @param containerIds
   *          a list of <code>ContainerId</code>s of containers for which to
   *          obtain the <code>ContainerStatus</code>
   */
  @Public
  @Stable
  public abstract void setContainerIds(List<ContainerId> containerIds);

  @Public
  @Stable
  public abstract void setCapability(Resource capabilty);

  @Public
  @Stable
  public abstract Resource getCapability();
}
