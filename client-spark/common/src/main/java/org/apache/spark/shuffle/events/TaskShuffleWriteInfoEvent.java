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

package org.apache.spark.shuffle.events;

import java.util.Map;

public class TaskShuffleWriteInfoEvent extends UniffleEvent {
  private int stageId;
  private int shuffleId;
  private long taskId;
  private Map<String, ShuffleWriteMetric> metrics;
  private ShuffleWriteTimes writeTimes;
  private boolean isShuffleWriteFailed;
  private String failureReason;
  private long uncompressedByteSize;

  public TaskShuffleWriteInfoEvent(
      int stageId,
      int shuffleId,
      long taskId,
      Map<String, ShuffleWriteMetric> metrics,
      ShuffleWriteTimes writeTimes,
      boolean isShuffleWriteFailed,
      String failureReason,
      long uncompressedByteSize) {
    this.stageId = stageId;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.metrics = metrics;
    this.writeTimes = writeTimes;
    this.isShuffleWriteFailed = isShuffleWriteFailed;
    this.failureReason = failureReason;
    this.uncompressedByteSize = uncompressedByteSize;
  }

  public int getStageId() {
    return stageId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public long getTaskId() {
    return taskId;
  }

  public Map<String, ShuffleWriteMetric> getMetrics() {
    return metrics;
  }

  public ShuffleWriteTimes getWriteTimes() {
    return writeTimes;
  }

  public boolean isShuffleWriteFailed() {
    return isShuffleWriteFailed;
  }

  public String getFailureReason() {
    return failureReason;
  }

  public long getUncompressedByteSize() {
    return uncompressedByteSize;
  }
}
