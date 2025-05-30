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

package org.apache.uniffle.storage.handler.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleDataSegment;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.segment.SegmentSplitterFactory;

public abstract class DataSkippableReadHandler extends PrefetchableClientReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DataSkippableReadHandler.class);

  protected List<ShuffleDataSegment> shuffleDataSegments = Lists.newArrayList();
  protected int segmentIndex = 0;

  protected Roaring64NavigableMap expectBlockIds;
  protected Set<Long> processBlockIds;

  protected ShuffleDataDistributionType distributionType;
  protected Roaring64NavigableMap expectTaskIds;

  public DataSkippableReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int readBufferSize,
      Roaring64NavigableMap expectBlockIds,
      Set<Long> processBlockIds,
      ShuffleDataDistributionType distributionType,
      Roaring64NavigableMap expectTaskIds,
      Optional<PrefetchOption> prefetchOption) {
    super(prefetchOption);
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.readBufferSize = readBufferSize;
    this.expectBlockIds = expectBlockIds;
    this.processBlockIds = processBlockIds;
    this.distributionType = distributionType;
    this.expectTaskIds = expectTaskIds;
  }

  protected abstract ShuffleIndexResult readShuffleIndex();

  protected abstract ShuffleDataResult readShuffleData(ShuffleDataSegment segment);

  @Override
  public ShuffleDataResult doReadShuffleData() {
    if (shuffleDataSegments.isEmpty()) {
      ShuffleIndexResult shuffleIndexResult = readShuffleIndex();
      if (shuffleIndexResult == null || shuffleIndexResult.isEmpty()) {
        return null;
      }

      try {
        shuffleDataSegments =
            SegmentSplitterFactory.getInstance()
                .get(distributionType, expectTaskIds, readBufferSize)
                .split(shuffleIndexResult);
      } finally {
        shuffleIndexResult.release();
      }
    }

    // We should skip unexpected and processed segments when handler is read
    ShuffleDataResult result = null;
    while (segmentIndex < shuffleDataSegments.size()) {
      ShuffleDataSegment segment = shuffleDataSegments.get(segmentIndex);
      Set<Long> blocksOfSegment = new HashSet<>();
      segment.getBufferSegments().forEach(block -> blocksOfSegment.add(block.getBlockId()));
      // skip unexpected blockIds
      blocksOfSegment.removeIf(blockId -> !expectBlockIds.contains(blockId));
      if (!blocksOfSegment.isEmpty()) {
        // skip processed blockIds
        blocksOfSegment.removeAll(processBlockIds);
        if (!blocksOfSegment.isEmpty()) {
          result = readShuffleData(segment);
          segmentIndex++;
          break;
        }
      }
      segmentIndex++;
    }
    return result;
  }
}
