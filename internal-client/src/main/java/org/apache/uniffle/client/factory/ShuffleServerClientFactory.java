/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.client.factory;

import java.util.Map;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcNettyClient;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.JavaUtils;

public class ShuffleServerClientFactory {

  private Map<String, Map<ShuffleServerInfo, ShuffleServerClient>> clients;

  private ShuffleServerClientFactory() {
    clients = JavaUtils.newConcurrentMap();
  }

  private static class LazyHolder {
    static final ShuffleServerClientFactory INSTANCE = new ShuffleServerClientFactory();
  }

  public static ShuffleServerClientFactory getInstance() {
    return LazyHolder.INSTANCE;
  }

  private ShuffleServerClient createShuffleServerClient(
      String clientType, ShuffleServerInfo shuffleServerInfo, RssConf rssConf) {
    if (clientType.equalsIgnoreCase(ClientType.GRPC.name())) {
      return new ShuffleServerGrpcClient(rssConf, shuffleServerInfo);
    } else if (clientType.equalsIgnoreCase(ClientType.GRPC_NETTY.name())) {
      return new ShuffleServerGrpcNettyClient(
          rssConf,
          shuffleServerInfo.getHost(),
          shuffleServerInfo.getGrpcPort(),
          shuffleServerInfo.getNettyPort());
    } else {
      throw new UnsupportedOperationException("Unsupported client type " + clientType);
    }
  }

  public ShuffleServerClient getShuffleServerClient(
      String clientType, ShuffleServerInfo shuffleServerInfo) {
    return getShuffleServerClient(clientType, shuffleServerInfo, new RssConf());
  }

  public ShuffleServerClient getShuffleServerClient(
      String clientType, ShuffleServerInfo shuffleServerInfo, RssConf rssConf) {
    Map<ShuffleServerInfo, ShuffleServerClient> serverToClients =
        clients.computeIfAbsent(clientType, key -> JavaUtils.newConcurrentMap());
    return serverToClients.computeIfAbsent(
        shuffleServerInfo,
        key -> createShuffleServerClient(clientType, shuffleServerInfo, rssConf));
  }

  // Only for tests
  public synchronized void cleanupCache() {
    clients.values().stream().flatMap(x -> x.values().stream()).forEach(ShuffleServerClient::close);
    this.clients = JavaUtils.newConcurrentMap();
  }
}
