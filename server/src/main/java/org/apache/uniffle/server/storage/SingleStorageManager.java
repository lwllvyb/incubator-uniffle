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

package org.apache.uniffle.server.storage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.storage.ApplicationStorageInfo;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.common.StorageWriteMetrics;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;

public abstract class SingleStorageManager implements StorageManager {

  private static final Logger LOG = LoggerFactory.getLogger(SingleStorageManager.class);

  private final long writeSlowThreshold;
  private final long eventSizeThresholdL1;
  private final long eventSizeThresholdL2;
  private final long eventSizeThresholdL3;
  protected final Map<String, ApplicationStorageInfo> appStorageInfoMap =
      JavaUtils.newConcurrentMap();

  public SingleStorageManager(ShuffleServerConf conf) {
    writeSlowThreshold = conf.getSizeAsBytes(ShuffleServerConf.SERVER_WRITE_SLOW_THRESHOLD);
    eventSizeThresholdL1 = conf.getSizeAsBytes(ShuffleServerConf.SERVER_EVENT_SIZE_THRESHOLD_L1);
    eventSizeThresholdL2 = conf.getSizeAsBytes(ShuffleServerConf.SERVER_EVENT_SIZE_THRESHOLD_L2);
    eventSizeThresholdL3 = conf.getSizeAsBytes(ShuffleServerConf.SERVER_EVENT_SIZE_THRESHOLD_L3);
  }

  @Override
  public boolean write(Storage storage, ShuffleWriteHandler handler, ShuffleDataFlushEvent event) {
    String shuffleKey = RssUtils.generateShuffleKey(event.getAppId(), event.getShuffleId());
    storage.createMetadataIfNotExist(shuffleKey);
    try {
      long startWrite = System.currentTimeMillis();
      handler.write(event.getShuffleBlocks());
      long writeTime = System.currentTimeMillis() - startWrite;
      updateWriteMetrics(event, writeTime);
      return true;
    } catch (Exception e) {
      LOG.warn("Exception happened when write data for " + event + ", try again", e);
      ShuffleServerMetrics.counterWriteException.inc();
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
    }
    return false;
  }

  @Override
  public void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    // the metrics update shouldn't block normal process
    // log the exception if error happen
    try {
      // update shuffle server metrics, these metrics belong to server module
      // we can't update them in storage module
      StorageWriteMetrics metrics = createStorageWriteMetrics(event, writeTime);
      ShuffleServerMetrics.counterTotalWriteTime.inc(metrics.getWriteTime());
      ShuffleServerMetrics.counterWriteTotal.inc();
      if (metrics.getWriteTime() > writeSlowThreshold) {
        ShuffleServerMetrics.counterWriteSlow.inc();
      }
      ShuffleServerMetrics.counterTotalWriteDataSize.inc(metrics.getDataSize());
      ShuffleServerMetrics.counterTotalWriteBlockSize.inc(metrics.getWriteBlocks());
      if (metrics.getEventSize() < eventSizeThresholdL1) {
        ShuffleServerMetrics.counterEventSizeThresholdLevel1.inc();
      } else if (metrics.getEventSize() < eventSizeThresholdL2) {
        ShuffleServerMetrics.counterEventSizeThresholdLevel2.inc();
      } else if (metrics.getEventSize() < eventSizeThresholdL3) {
        ShuffleServerMetrics.counterEventSizeThresholdLevel3.inc();
      } else {
        ShuffleServerMetrics.counterEventSizeThresholdLevel4.inc();
      }
      String appId = event.getAppId();
      ApplicationStorageInfo appStorage =
          appStorageInfoMap.computeIfAbsent(appId, id -> new ApplicationStorageInfo(appId));
      appStorage.incUsedBytes(event.getDataLength());
      ShuffleServerMetrics.gaugeStorageUsedBytes.inc(event.getDataLength());
      if (event.getUnderStorage().containsWriteHandler(appId)) {
        appStorage.incFileNum(1);
        ShuffleServerMetrics.gaugeFlushFileNum.inc();
        ShuffleServerMetrics.counterTotalFlushFileNum.inc();
      }
      Storage storage = event.getUnderStorage();
      if (storage != null) {
        storage.updateWriteMetrics(metrics);
      }
    } catch (Exception e) {
      LOG.warn("Exception happened when update write metrics for " + event, e);
    }
  }

  @Override
  public boolean canWrite(ShuffleDataFlushEvent event) {
    try {
      Storage storage = selectStorage(event);
      // if storage is null, appId may not be registered
      return storage != null && storage.canWrite();
    } catch (Exception e) {
      LOG.warn("Exception happened when select storage", e);
      return false;
    }
  }

  public StorageWriteMetrics createStorageWriteMetrics(
      ShuffleDataFlushEvent event, long writeTime) {
    long blockNum = event.getShuffleBlocks().size();
    List<Integer> partitions = Lists.newArrayList();
    for (int partition = event.getStartPartition();
        partition <= event.getEndPartition();
        partition++) {
      partitions.add(partition);
    }
    return new StorageWriteMetrics(
        event.getEncodedLength(),
        blockNum,
        writeTime,
        event.getDataLength(),
        partitions,
        event.getAppId(),
        event.getShuffleId());
  }

  @Override
  public void start() {
    // do nothing
  }

  @Override
  public void stop() {
    // do nothing
  }

  public void removeAppStorageInfo(PurgeEvent event) {
    String appId = event.getAppId();
    ApplicationStorageInfo info = appStorageInfoMap.remove(appId);
    if (info != null) {
      ShuffleServerMetrics.gaugeStorageUsedBytes.dec(info.getUsedBytes());
      ShuffleServerMetrics.gaugeFlushFileNum.dec(info.getFileNum());
      ShuffleServerMetrics.counterTotalDeleteDataSize.inc(info.getUsedBytes());
      ShuffleServerMetrics.counterTotalDeleteFileNum.inc(info.getFileNum());
    }
  }
}
