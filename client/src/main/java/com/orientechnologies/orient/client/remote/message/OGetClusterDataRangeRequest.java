/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

public class OGetClusterDataRangeRequest implements OBinaryRequest<OGetClusterDataRangeResponse> {
  private int clusterId;

  public OGetClusterDataRangeRequest(int iClusterId) {
    this.clusterId = iClusterId;
  }

  public OGetClusterDataRangeRequest() {
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeShort((short) clusterId);
  }

  public void read(OChannelDataInput channel, int protocolVersion, String serializerName) throws IOException {
    this.clusterId = channel.readShort();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_DATACLUSTER_DATARANGE;
  }

  @Override
  public String getDescription() {
    return "Get the begin/end range of data in cluster";
  }

  public int getClusterId() {
    return clusterId;
  }

  @Override
  public OGetClusterDataRangeResponse createResponse() {
    return new OGetClusterDataRangeResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeClusterDataRange(this);
  }

}