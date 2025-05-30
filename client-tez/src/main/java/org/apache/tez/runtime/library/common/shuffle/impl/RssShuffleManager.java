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

package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.InputContextUtils;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.UmbilicalUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.FetchResult;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.Fetcher;
import org.apache.tez.runtime.library.common.shuffle.Fetcher.FetcherBuilder;
import org.apache.tez.runtime.library.common.shuffle.HostPort;
import org.apache.tez.runtime.library.common.shuffle.InputHost;
import org.apache.tez.runtime.library.common.shuffle.InputHost.PartitionToInputs;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils.FetchStatsLogger;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.JavaUtils;

// This only knows how to deal with a single srcIndex for a given targetIndex.
// In case the src task generates multiple outputs for the same target Index
// (multiple src-indices), modifications will be required.
public class RssShuffleManager extends ShuffleManager {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManager.class);
  private static final Logger LOG_FETCH = LoggerFactory.getLogger(LOG.getName() + ".fetch");
  private static final FetchStatsLogger fetchStatsLogger = new FetchStatsLogger(LOG_FETCH, LOG);

  private final InputContext inputContext;
  private final int numInputs;
  private final int shuffleId;
  private final ApplicationAttemptId applicationAttemptId;

  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  private final FetchedInputAllocator inputManager;

  @VisibleForTesting final ListeningExecutorService fetcherExecutor;

  /** Executor for ReportCallable. */
  private ExecutorService reporterExecutor;

  /** Lock to sync failedEvents. */
  private final ReentrantLock reportLock = new ReentrantLock();

  /** Condition to wake up the thread notifying when events fail. */
  private final Condition reportCondition = reportLock.newCondition();

  /** Events reporting fetcher failed. */
  private final HashMap<InputReadErrorEvent, Integer> failedEvents = new HashMap<>();

  private final ListeningExecutorService schedulerExecutor;
  private final RssRunShuffleCallable rssSchedulerCallable;

  private final BlockingQueue<FetchedInput> completedInputs;
  private final AtomicBoolean inputReadyNotificationSent = new AtomicBoolean(false);
  @VisibleForTesting final BitSet completedInputSet;
  private final ConcurrentMap<HostPort, InputHost> knownSrcHosts;
  private final BlockingQueue<InputHost> pendingHosts;
  private final Set<InputAttemptIdentifier> obsoletedInputs;
  private Set<RssTezFetcherTask> rssRunningFetchers;

  private final AtomicInteger numCompletedInputs = new AtomicInteger(0);
  private final AtomicInteger numFetchedSpills = new AtomicInteger(0);

  private final long startTime;
  private long lastProgressTime;
  private long totalBytesShuffledTillNow;

  // Required to be held when manipulating pendingHosts
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition wakeLoop = lock.newCondition();

  private final int numFetchers;
  private final boolean asyncHttp;

  // Parameters required by Fetchers
  private final CompressionCodec codec;
  private final Configuration conf;
  private final boolean localDiskFetchEnabled;
  private final boolean sharedFetchEnabled;
  private final boolean verifyDiskChecksum;
  private final boolean compositeFetch;

  private final int ifileBufferSize;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;

  /** Holds the time to wait for failures to batch them and send less events. */
  private final int maxTimeToWaitForReportMillis;

  private final String srcNameTrimmed;

  private final int maxTaskOutputAtOnce;

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private final TezCounter shuffledInputsCounter;
  private final TezCounter failedShufflesCounter;
  private final TezCounter bytesShuffledCounter;
  private final TezCounter decompressedDataSizeCounter;
  private final TezCounter bytesShuffledToDiskCounter;
  private final TezCounter bytesShuffledToMemCounter;
  private final TezCounter bytesShuffledDirectDiskCounter;

  private volatile Throwable shuffleError;
  private final HttpConnectionParams httpConnectionParams;

  private final LocalDirAllocator localDirAllocator;
  private final RawLocalFileSystem localFs;
  private final Path[] localDisks;
  private final String localhostName;
  private final int shufflePort;

  private final TezCounter shufflePhaseTime;
  private final TezCounter firstEventReceived;
  private final TezCounter lastEventReceived;

  // To track shuffleInfo events when finalMerge is disabled OR pipelined shuffle is enabled in
  // source.
  @VisibleForTesting final Map<Integer, ShuffleEventInfo> shuffleInfoEventsMap;

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private final Set<Integer> successRssPartitionSet = new HashSet<>();
  private final Set<Integer> runningRssPartitionMap = new HashSet<>();
  private final Set<Integer> allRssPartition = Sets.newConcurrentHashSet();
  private final BlockingQueue<Integer> pendingPartition = new LinkedBlockingQueue<>();
  Map<Integer, List<InputAttemptIdentifier>> partitionToInput = new HashMap<>();
  private final Map<Integer, Roaring64NavigableMap> rssAllBlockIdBitmapMap =
      JavaUtils.newConcurrentMap();
  private final Map<Integer, Roaring64NavigableMap> rssSuccessBlockIdBitmapMap =
      JavaUtils.newConcurrentMap();
  private final AtomicInteger numNoDataInput = new AtomicInteger(0);
  private final AtomicInteger numWithDataInput = new AtomicInteger(0);

  public RssShuffleManager(
      InputContext inputContext,
      Configuration conf,
      int numInputs,
      int bufferSize,
      boolean ifileReadAheadEnabled,
      int ifileReadAheadLength,
      CompressionCodec codec,
      FetchedInputAllocator inputAllocator,
      int shuffleId,
      ApplicationAttemptId applicationAttemptId)
      throws IOException {
    super(
        inputContext,
        conf,
        numInputs,
        bufferSize,
        ifileReadAheadEnabled,
        ifileReadAheadLength,
        codec,
        inputAllocator);
    this.inputContext = inputContext;
    this.conf = conf;
    this.numInputs = numInputs;
    this.shuffleId = shuffleId;
    this.applicationAttemptId = applicationAttemptId;

    this.shuffledInputsCounter =
        inputContext.getCounters().findCounter(TaskCounter.NUM_SHUFFLED_INPUTS);
    this.failedShufflesCounter =
        inputContext.getCounters().findCounter(TaskCounter.NUM_FAILED_SHUFFLE_INPUTS);
    this.bytesShuffledCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES);
    this.decompressedDataSizeCounter =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DECOMPRESSED);
    this.bytesShuffledToDiskCounter =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_DISK);
    this.bytesShuffledToMemCounter =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_MEM);
    this.bytesShuffledDirectDiskCounter =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DISK_DIRECT);

    this.ifileBufferSize = bufferSize;
    this.ifileReadAhead = ifileReadAheadEnabled;
    this.ifileReadAheadLength = ifileReadAheadLength;
    this.codec = codec;
    this.inputManager = inputAllocator;
    this.localDiskFetchEnabled =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH,
            TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT);
    this.sharedFetchEnabled =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH,
            TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH_DEFAULT);
    this.verifyDiskChecksum =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM_DEFAULT);
    this.maxTimeToWaitForReportMillis = 1;

    this.shufflePhaseTime = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_PHASE_TIME);
    this.firstEventReceived =
        inputContext.getCounters().findCounter(TaskCounter.FIRST_EVENT_RECEIVED);
    this.lastEventReceived =
        inputContext.getCounters().findCounter(TaskCounter.LAST_EVENT_RECEIVED);
    this.compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);

    this.srcNameTrimmed = TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName());

    completedInputSet = new BitSet(numInputs);
    /**
     * In case of pipelined shuffle, it is possible to get multiple FetchedInput per attempt. We do
     * not know upfront the number of spills from source.
     */
    completedInputs = new LinkedBlockingDeque<>();
    knownSrcHosts = JavaUtils.newConcurrentMap();
    pendingHosts = new LinkedBlockingQueue<>();
    obsoletedInputs = Collections.newSetFromMap(JavaUtils.newConcurrentMap());
    rssRunningFetchers = Collections.newSetFromMap(JavaUtils.newConcurrentMap());

    int maxConfiguredFetchers =
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);

    this.numFetchers = Math.min(maxConfiguredFetchers, numInputs);

    final ExecutorService fetcherRawExecutor;
    if (conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL_DEFAULT)) {
      fetcherRawExecutor =
          inputContext.createTezFrameworkExecutorService(
              numFetchers, "Fetcher_B {" + srcNameTrimmed + "} #%d");
    } else {
      fetcherRawExecutor =
          Executors.newFixedThreadPool(
              numFetchers,
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat("Fetcher_B {" + srcNameTrimmed + "} #%d")
                  .build());
    }
    this.fetcherExecutor = MoreExecutors.listeningDecorator(fetcherRawExecutor);

    ExecutorService schedulerRawExecutor =
        Executors.newFixedThreadPool(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("ShuffleRunner {" + srcNameTrimmed + "}")
                .build());
    this.schedulerExecutor = MoreExecutors.listeningDecorator(schedulerRawExecutor);
    this.rssSchedulerCallable = new RssRunShuffleCallable(conf);

    this.startTime = System.currentTimeMillis();
    this.lastProgressTime = startTime;

    this.asyncHttp =
        conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_USE_ASYNC_HTTP, false);
    httpConnectionParams = ShuffleUtils.getHttpConnectionParams(conf);

    this.localFs = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();

    this.localDirAllocator = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);

    this.localDisks =
        Iterables.toArray(localDirAllocator.getAllLocalPathsToRead(".", conf), Path.class);
    this.localhostName = inputContext.getExecutionContext().getHostName();

    String auxiliaryService =
        conf.get(
            TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
            TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
    final ByteBuffer shuffleMetaData = inputContext.getServiceProviderMetaData(auxiliaryService);
    this.shufflePort = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetaData);

    /**
     * Setting to very high val can lead to Http 400 error. Cap it to 75; every attempt id would be
     * approximately 48 bytes; 48 * 75 = 3600 which should give some room for other info in URL.
     */
    this.maxTaskOutputAtOnce =
        Math.max(
            1,
            Math.min(
                75,
                conf.getInt(
                    TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE,
                    TezRuntimeConfiguration
                        .TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE_DEFAULT)));

    if (null != this.localDisks) {
      Arrays.sort(this.localDisks);
    }

    shuffleInfoEventsMap = JavaUtils.newConcurrentMap();

    LOG.info(
        srcNameTrimmed
            + ": numInputs="
            + numInputs
            + ", compressionCodec="
            + (codec == null ? "NoCompressionCodec" : codec.getClass().getName())
            + ", numFetchers="
            + numFetchers
            + ", ifileBufferSize="
            + ifileBufferSize
            + ", ifileReadAheadEnabled="
            + ifileReadAhead
            + ", ifileReadAheadLength="
            + ifileReadAheadLength
            + ", "
            + "localDiskFetchEnabled="
            + localDiskFetchEnabled
            + ", "
            + "sharedFetchEnabled="
            + sharedFetchEnabled
            + ", "
            + httpConnectionParams.toString()
            + ", maxTaskOutputAtOnce="
            + maxTaskOutputAtOnce);
  }

  @Override
  public void run() throws IOException {
    TezTaskAttemptID tezTaskAttemptId = InputContextUtils.getTezTaskAttemptID(this.inputContext);
    this.partitionToServers =
        UmbilicalUtils.requestShuffleServer(
            this.inputContext.getApplicationId(), this.conf, tezTaskAttemptId, shuffleId);

    Preconditions.checkState(inputManager != null, "InputManager must be configured");
    if (maxTimeToWaitForReportMillis > 0) {
      reporterExecutor =
          Executors.newSingleThreadExecutor(
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat("ShuffleRunner {" + srcNameTrimmed + "}")
                  .build());
      Future reporterFuture = reporterExecutor.submit(new ReporterCallable());
    }

    ListenableFuture<Void> runShuffleFuture = schedulerExecutor.submit(rssSchedulerCallable);
    Futures.addCallback(
        runShuffleFuture, new SchedulerFutureCallback(), MoreExecutors.directExecutor());
    // Shutdown this executor once this task, and the callback complete.
    schedulerExecutor.shutdown();
  }

  private class ReporterCallable extends CallableWithNdc<Void> {
    /** Measures if the batching interval has ended. */
    ReporterCallable() {}

    @Override
    protected Void callInternal() throws Exception {
      long nextReport = 0;
      while (!isShutdown.get()) {
        reportLock.lock();
        try {
          while (failedEvents.isEmpty()) {
            boolean signaled =
                reportCondition.await(maxTimeToWaitForReportMillis, TimeUnit.MILLISECONDS);
          }

          long currentTime = Time.monotonicNow();
          ;
          if (currentTime > nextReport) {
            if (failedEvents.size() > 0) {
              List<Event> failedEventsToSend = Lists.newArrayListWithCapacity(failedEvents.size());
              for (InputReadErrorEvent key : failedEvents.keySet()) {
                failedEventsToSend.add(
                    InputReadErrorEvent.create(
                        key.getDiagnostics(), key.getIndex(), key.getVersion()));
              }
              inputContext.sendEvents(failedEventsToSend);
              failedEvents.clear();
              nextReport = currentTime + maxTimeToWaitForReportMillis;
            }
          }
        } finally {
          reportLock.unlock();
        }
      }
      return null;
    }
  }

  private boolean isAllInputFetched() {
    LOG.info(
        "Check isAllInputFetched, numNoDataInput:{}, numWithDataInput:{},numInputs:{},  "
            + "successRssPartitionSet:{},  allRssPartition:{}.",
        numNoDataInput,
        numWithDataInput,
        numInputs,
        successRssPartitionSet,
        allRssPartition);
    return (numNoDataInput.get() + numWithDataInput.get() >= numInputs)
        && (successRssPartitionSet.size() >= allRssPartition.size());
  }

  private boolean isAllInputAdded() {
    LOG.info(
        "Check isAllInputAdded, numNoDataInput:{}, numWithDataInput:{},numInputs:{},  "
            + "successRssPartitionSet:{}, allRssPartition:{}.",
        numNoDataInput,
        numWithDataInput,
        numInputs,
        successRssPartitionSet,
        allRssPartition);
    return numNoDataInput.get() + numWithDataInput.get() >= numInputs;
  }

  private class RssRunShuffleCallable extends CallableWithNdc<Void> {

    private final Configuration conf;

    RssRunShuffleCallable(Configuration conf) {
      this.conf = conf;
    }

    @Override
    protected Void callInternal() throws Exception {
      while (!isShutdown.get() && !isAllInputFetched()) {
        lock.lock();
        try {
          LOG.info(
              "numFetchers:{}, shuffleInfoEventsMap.size:{}, numInputs:{}.",
              numFetchers,
              shuffleInfoEventsMap.size(),
              numInputs);
          while (((rssRunningFetchers.size() >= numFetchers || pendingPartition.isEmpty())
                  && !isAllInputFetched())
              || !isAllInputAdded()) {
            LOG.info(
                "isAllInputAdded:{}, rssRunningFetchers:{}, numFetchers:{}, pendingPartition:{}, "
                    + "successRssPartitionSet:{}, allRssPartition:{} ",
                isAllInputAdded(),
                rssRunningFetchers,
                numFetchers,
                pendingPartition,
                successRssPartitionSet,
                allRssPartition);

            inputContext.notifyProgress();
            boolean isSignal = wakeLoop.await(1000, TimeUnit.MILLISECONDS);
            if (isSignal) {
              LOG.info("wakeLoop is signal");
            }
            if (isShutdown.get()) {
              LOG.info("is shut down and break");
              break;
            }
          }
          LOG.info(
              "run out of while, is all inputadded:{}, fetched:{}",
              isAllInputAdded(),
              isAllInputFetched());
        } finally {
          lock.unlock();
        }

        if (shuffleError != null) {
          LOG.warn("Shuffle error.", shuffleError);
          // InputContext has already been informed of a fatal error. Relying on
          // tez to kill the task.
          break;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: NumCompletedInputs: {}", srcNameTrimmed, numCompletedInputs);
        }

        if (!isAllInputFetched() && !isShutdown.get()) {
          lock.lock();
          try {
            LOG.info(
                "numFetchers:{}，runningFetchers.size():{}.",
                numFetchers,
                rssRunningFetchers.size());
            int maxFetchersToRun = numFetchers - rssRunningFetchers.size();
            int count = 0;
            LOG.info("pendingPartition:{}", pendingPartition.peek());
            while (pendingPartition.peek() != null && !isShutdown.get()) {
              Integer partition = null;
              try {
                partition = pendingPartition.take();
              } catch (InterruptedException e) {
                if (isShutdown.get()) {
                  LOG.info(
                      srcNameTrimmed
                          + ": "
                          + "Interrupted and hasBeenShutdown, Breaking out of ShuffleScheduler");
                  Thread.currentThread().interrupt();
                  break;
                } else {
                  throw e;
                }
              }

              if (LOG.isDebugEnabled()) {
                LOG.debug("{}: Processing pending partition: {}", srcNameTrimmed, partition);
              }

              if (!isShutdown.get()
                  && (!successRssPartitionSet.contains(partition)
                      && !runningRssPartitionMap.contains(partition))) {
                runningRssPartitionMap.add(partition);
                LOG.info(
                    "generate RssTezFetcherTask, partition:{}, rssWoker:{}, all woker:{}",
                    partition,
                    partitionToServers.get(partition),
                    partitionToServers);

                int maxAttemptNo = RssTezUtils.getMaxAttemptNo(conf);

                RssTezFetcherTask fetcher =
                    new RssTezFetcherTask(
                        RssShuffleManager.this,
                        inputContext,
                        conf,
                        inputManager,
                        partition,
                        shuffleId,
                        applicationAttemptId,
                        partitionToInput.get(partition),
                        new HashSet<ShuffleServerInfo>(partitionToServers.get(partition)),
                        rssAllBlockIdBitmapMap,
                        rssSuccessBlockIdBitmapMap,
                        numInputs,
                        partitionToServers.size(),
                        maxAttemptNo);
                rssRunningFetchers.add(fetcher);
                if (isShutdown.get()) {
                  LOG.info(
                      srcNameTrimmed
                          + ": "
                          + "hasBeenShutdown,"
                          + "Breaking out of ShuffleScheduler Loop");
                  break;
                }
                ListenableFuture<FetchResult> future =
                    fetcherExecutor.submit(fetcher); // add fetcher task
                Futures.addCallback(
                    future, new FetchFutureCallback(fetcher), MoreExecutors.directExecutor());
                if (++count >= maxFetchersToRun) {
                  break;
                }
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug(
                      srcNameTrimmed
                          + ": "
                          + "Skipping partition: "
                          + partition
                          + " since is shutdown");
                }
              }
            }
          } finally {
            lock.unlock();
          }
        }
      }
      LOG.info("RssShuffleManager numInputs:{}", numInputs);
      shufflePhaseTime.setValue(System.currentTimeMillis() - startTime);
      LOG.info(
          srcNameTrimmed
              + ": "
              + "Shutting down FetchScheduler, Was Interrupted: "
              + Thread.currentThread().isInterrupted());
      if (!fetcherExecutor.isShutdown()) {
        fetcherExecutor.shutdownNow();
      }
      return null;
    }
  }

  private boolean validateInputAttemptForPipelinedShuffle(InputAttemptIdentifier input) {
    // For pipelined shuffle.
    // TEZ-2132 for error handling. As of now, fail fast if there is a different attempt
    if (input.canRetrieveInputInChunks()) {
      ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(input.getInputIdentifier());
      if (eventInfo != null && input.getAttemptNumber() != eventInfo.attemptNum) {
        if (eventInfo.scheduledForDownload || !eventInfo.eventsProcessed.isEmpty()) {
          IOException exception =
              new IOException(
                  "Previous event already got scheduled for "
                      + input
                      + ". Previous attempt's data could have been already merged "
                      + "to memory/disk outputs.  Killing (self) this task early."
                      + " currentAttemptNum="
                      + eventInfo.attemptNum
                      + ", eventsProcessed="
                      + eventInfo.eventsProcessed
                      + ", scheduledForDownload="
                      + eventInfo.scheduledForDownload
                      + ", newAttemptNum="
                      + input.getAttemptNumber());
          String message = "Killing self as previous attempt data could have been consumed";
          killSelf(exception, message);
          return false;
        }
      }
    }
    return true;
  }

  @Override
  void killSelf(Exception exception, String message) {
    LOG.error(message, exception);
    this.inputContext.killSelf(exception, message);
  }

  @VisibleForTesting
  @Override
  Fetcher constructFetcherForHost(InputHost inputHost, Configuration conf) {
    Path lockDisk = null;

    if (sharedFetchEnabled) {
      // pick a single lock disk from the edge name's hashcode + host hashcode
      final int h = Math.abs(Objects.hashCode(this.srcNameTrimmed, inputHost.getHost()));
      lockDisk = new Path(this.localDisks[h % this.localDisks.length], "locks");
    }

    FetcherBuilder fetcherBuilder =
        new FetcherBuilder(
            RssShuffleManager.this,
            httpConnectionParams,
            inputManager,
            inputContext.getApplicationId(),
            inputContext.getDagIdentifier(),
            null,
            srcNameTrimmed,
            conf,
            localFs,
            localDirAllocator,
            lockDisk,
            localDiskFetchEnabled,
            sharedFetchEnabled,
            localhostName,
            shufflePort,
            asyncHttp,
            verifyDiskChecksum,
            compositeFetch);

    if (codec != null) {
      fetcherBuilder.setCompressionParameters(codec);
    }
    fetcherBuilder.setIFileParams(ifileReadAhead, ifileReadAheadLength);

    // Remove obsolete inputs from the list being given to the fetcher. Also
    // remove from the obsolete list.
    PartitionToInputs pendingInputsOfOnePartitionRange = inputHost.clearAndGetOnePartitionRange();
    int includedMaps = 0;
    for (Iterator<InputAttemptIdentifier> inputIter =
            pendingInputsOfOnePartitionRange.getInputs().iterator();
        inputIter.hasNext(); ) {
      InputAttemptIdentifier input = inputIter.next();

      // For pipelined shuffle.
      if (!validateInputAttemptForPipelinedShuffle(input)) {
        continue;
      }

      // Avoid adding attempts which have already completed.
      boolean alreadyCompleted;
      if (input instanceof CompositeInputAttemptIdentifier) {
        CompositeInputAttemptIdentifier compositeInput = (CompositeInputAttemptIdentifier) input;
        int nextClearBit = completedInputSet.nextClearBit(compositeInput.getInputIdentifier());
        int maxClearBit =
            compositeInput.getInputIdentifier() + compositeInput.getInputIdentifierCount();
        alreadyCompleted = nextClearBit > maxClearBit;
      } else {
        alreadyCompleted = completedInputSet.get(input.getInputIdentifier());
      }
      // Avoid adding attempts which have already completed or have been marked as OBSOLETE
      if (alreadyCompleted || obsoletedInputs.contains(input)) {
        inputIter.remove();
        continue;
      }

      // Check if max threshold is met
      if (includedMaps >= maxTaskOutputAtOnce) {
        inputIter.remove();
        // add to inputHost
        inputHost.addKnownInput(
            pendingInputsOfOnePartitionRange.getPartition(),
            pendingInputsOfOnePartitionRange.getPartitionCount(),
            input);
      } else {
        includedMaps++;
      }
    }
    if (inputHost.getNumPendingPartitions() > 0) {
      pendingHosts.add(inputHost); // add it to queue
    }
    for (InputAttemptIdentifier input : pendingInputsOfOnePartitionRange.getInputs()) {
      ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(input.getInputIdentifier());
      if (eventInfo != null) {
        eventInfo.scheduledForDownload = true;
      }
    }
    fetcherBuilder.assignWork(
        inputHost.getHost(),
        inputHost.getPort(),
        pendingInputsOfOnePartitionRange.getPartition(),
        pendingInputsOfOnePartitionRange.getPartitionCount(),
        pendingInputsOfOnePartitionRange.getInputs());
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Created Fetcher for host: "
              + inputHost.getHost()
              + ", info: "
              + inputHost.getAdditionalInfo()
              + ", with inputs: "
              + pendingInputsOfOnePartitionRange);
    }
    return fetcherBuilder.build();
  }

  /////////////////// Methods for InputEventHandler
  @Override
  public void addKnownInput(
      String hostName,
      int port,
      CompositeInputAttemptIdentifier srcAttemptIdentifier,
      int srcPhysicalIndex) {
    HostPort identifier = new HostPort(hostName, port);
    InputHost host = knownSrcHosts.get(identifier);
    if (host == null) {
      host = new InputHost(identifier);
      InputHost old = knownSrcHosts.putIfAbsent(identifier, host);
      if (old != null) {
        host = old;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: Adding input: {}, to host: {}", srcNameTrimmed, srcAttemptIdentifier, host);
    }

    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier)) {
      return;
    }
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    for (int i = 0; i < srcAttemptIdentifier.getInputIdentifierCount(); i++) {
      if (shuffleInfoEventsMap.get(inputIdentifier + i) == null) {
        shuffleInfoEventsMap.put(
            inputIdentifier + i, new ShuffleEventInfo(srcAttemptIdentifier.expand(i)));
        LOG.info(
            "AddKnownInput, srcAttemptIdentifier:{}, i:{}, expand:{}, map:{}",
            srcAttemptIdentifier,
            i,
            srcAttemptIdentifier.expand(i),
            shuffleInfoEventsMap);
      }
    }

    host.addKnownInput(
        srcPhysicalIndex, srcAttemptIdentifier.getInputIdentifierCount(), srcAttemptIdentifier);
    lock.lock();
    try {
      boolean added = pendingHosts.offer(host);
      if (!added) {
        String errorMessage = "Unable to add host: " + host.getIdentifier() + " to pending queue";
        LOG.error(errorMessage);
        throw new TezUncheckedException(errorMessage);
      }
      wakeLoop.signal();
    } finally {
      lock.unlock();
    }

    LOG.info(
        "AddKnowInput, hostname:{}, port:{}, srcAttemptIdentifier:{}, srcPhysicalIndex:{}",
        hostName,
        port,
        srcAttemptIdentifier,
        srcPhysicalIndex);

    lock.lock();
    try {
      for (int i = 0; i < srcAttemptIdentifier.getInputIdentifierCount(); i++) {
        int p = srcPhysicalIndex + i;
        LOG.info(
            "PartitionToInput, original:{}, add:{},  now:{}",
            srcAttemptIdentifier,
            srcAttemptIdentifier.expand(i),
            partitionToInput.get(p));
        if (!allRssPartition.contains(srcPhysicalIndex + i)) {
          pendingPartition.add(p);
        }
        allRssPartition.add(p);
        partitionToInput.computeIfAbsent(p, key -> new ArrayList<>());
        partitionToInput.get(p).add(srcAttemptIdentifier);
        LOG.info("Add partition:{}, after add, now partition:{}", p, allRssPartition);
      }

      numWithDataInput.incrementAndGet();
      LOG.info("numWithDataInput:{}.", numWithDataInput.get());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void addCompletedInputWithNoData(InputAttemptIdentifier srcAttemptIdentifier) {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("No input data exists for SrcTask: " + inputIdentifier + ". Marking as complete.");
    }
    lock.lock();
    try {
      if (!completedInputSet.get(inputIdentifier)) {
        NullFetchedInput fetchedInput = new NullFetchedInput(srcAttemptIdentifier);
        if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
          registerCompletedInput(fetchedInput);
        } else {
          registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
        }
      }
      // Awake the loop to check for termination.
      wakeLoop.signal();
    } finally {
      lock.unlock();
    }
    numNoDataInput.incrementAndGet();
    LOG.info(
        "AddCompletedInputWithNoData, numNoDataInput:{}, numWithDataInput:{},numInputs:{},  "
            + "successRssPartitionSet:{}, allRssPartition:{}.",
        numNoDataInput,
        numWithDataInput,
        numInputs,
        successRssPartitionSet,
        allRssPartition);
  }

  @Override
  protected synchronized void updateEventReceivedTime() {
    long relativeTime = System.currentTimeMillis() - startTime;
    if (firstEventReceived.getValue() == 0) {
      firstEventReceived.setValue(relativeTime);
      lastEventReceived.setValue(relativeTime);
      return;
    }
    lastEventReceived.setValue(relativeTime);
  }

  @Override
  void obsoleteKnownInput(InputAttemptIdentifier srcAttemptIdentifier) {
    obsoletedInputs.add(srcAttemptIdentifier);
    // NEWTEZ Maybe inform the fetcher about this. For now, this is used during the initial fetch
    // list construction.
  }

  // End of Methods for InputEventHandler
  // Methods from FetcherCallbackHandler

  /**
   * Placeholder for tracking shuffle events in case we get multiple spills info for the same
   * attempt.
   */
  static class ShuffleEventInfo {
    BitSet eventsProcessed;
    int finalEventId = -1; // 0 indexed
    int attemptNum;
    String id;
    boolean scheduledForDownload; // whether chunks got scheduled for download

    ShuffleEventInfo(InputAttemptIdentifier input) {
      this.id = input.getInputIdentifier() + "_" + input.getAttemptNumber();
      this.eventsProcessed = new BitSet();
      this.attemptNum = input.getAttemptNumber();
    }

    void spillProcessed(int spillId) {
      if (finalEventId != -1) {
        Preconditions.checkState(
            eventsProcessed.cardinality() <= (finalEventId + 1),
            "Wrong state. eventsProcessed cardinality="
                + eventsProcessed.cardinality()
                + " "
                + "finalEventId="
                + finalEventId
                + ", spillId="
                + spillId
                + ", "
                + toString());
      }
      eventsProcessed.set(spillId);
    }

    void setFinalEventId(int spillId) {
      finalEventId = spillId;
    }

    boolean isDone() {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "finalEventId="
                + finalEventId
                + ", eventsProcessed cardinality="
                + eventsProcessed.cardinality());
      }
      return ((finalEventId != -1) && (finalEventId + 1) == eventsProcessed.cardinality());
    }

    @Override
    public String toString() {
      return "[eventsProcessed="
          + eventsProcessed
          + ", finalEventId="
          + finalEventId
          + ", id="
          + id
          + ", attemptNum="
          + attemptNum
          + ", scheduledForDownload="
          + scheduledForDownload
          + "]";
    }
  }

  @Override
  public void fetchSucceeded(
      String host,
      InputAttemptIdentifier srcAttemptIdentifier,
      FetchedInput fetchedInput,
      long fetchedBytes,
      long decompressedLength,
      long copyDuration)
      throws IOException {
    // Count irrespective of whether this is a copy of an already fetched input
    lock.lock();
    try {
      lastProgressTime = System.currentTimeMillis();
      inputContext.notifyProgress();
      fetchedInput.commit();
      fetchStatsLogger.logIndividualFetchComplete(
          copyDuration,
          fetchedBytes,
          decompressedLength,
          fetchedInput.getType().toString(),
          srcAttemptIdentifier);

      // Processing counters for completed and commit fetches only. Need
      // additional counters for excessive fetches - which primarily comes
      // in after speculation or retries.
      shuffledInputsCounter.increment(1);
      bytesShuffledCounter.increment(fetchedBytes);
      if (fetchedInput.getType() == FetchedInput.Type.MEMORY) {
        bytesShuffledToMemCounter.increment(fetchedBytes);
      } else if (fetchedInput.getType() == FetchedInput.Type.DISK) {
        LOG.warn("Rss bytesShuffledToDiskCounter");
        bytesShuffledToDiskCounter.increment(fetchedBytes);
      } else if (fetchedInput.getType() == FetchedInput.Type.DISK_DIRECT) {
        LOG.warn("Rss bytesShuffledDirectDiskCounter");
        bytesShuffledDirectDiskCounter.increment(fetchedBytes);
      }
      decompressedDataSizeCounter.increment(decompressedLength);

      if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
        registerCompletedInput(fetchedInput);
      } else {
        LOG.warn("Rss registerCompletedInputForPipelinedShuffle");
        registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
      }

      totalBytesShuffledTillNow += fetchedBytes;
      logProgress();
      wakeLoop.signal();

    } finally {
      lock.unlock();
    }
    // NEWTEZ Maybe inform fetchers, in case they have an alternate attempt of the same task in
    // their queue.
  }

  private void registerCompletedInput(FetchedInput fetchedInput) {
    lock.lock();
    try {
      maybeInformInputReady(fetchedInput);
      adjustCompletedInputs(fetchedInput);
      numFetchedSpills.getAndIncrement();
    } finally {
      lock.unlock();
    }
  }

  private void maybeInformInputReady(FetchedInput fetchedInput) {
    lock.lock();
    try {
      if (!(fetchedInput instanceof NullFetchedInput)) {
        LOG.info("maybeInformInputReady");
        completedInputs.add(fetchedInput);
      }
      if (!inputReadyNotificationSent.getAndSet(true)) {
        // Should eventually be controlled by Inputs which are processing the data.
        LOG.info("maybeInformInputReady InputContext inputIsReady");
        inputContext.inputIsReady();
      }
    } finally {
      lock.unlock();
    }
  }

  private void adjustCompletedInputs(FetchedInput fetchedInput) {
    lock.lock();
    try {
      completedInputSet.set(fetchedInput.getInputAttemptIdentifier().getInputIdentifier());
      int numComplete = numCompletedInputs.incrementAndGet();
      LOG.info("AdjustCompletedInputs, numCompletedInputs:{}", numComplete);
    } finally {
      lock.unlock();
    }
  }

  private void registerCompletedInputForPipelinedShuffle(
      InputAttemptIdentifier srcAttemptIdentifier, FetchedInput fetchedInput) {
    /**
     * For pipelinedshuffle it is possible to get multiple spills. Claim success only when all
     * spills pertaining to an attempt are done.
     */
    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier)) {
      return;
    }

    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);

    // for empty partition case
    if (eventInfo == null && fetchedInput instanceof NullFetchedInput) {
      eventInfo = new ShuffleEventInfo(srcAttemptIdentifier);
      shuffleInfoEventsMap.put(inputIdentifier, eventInfo);
    }

    if (eventInfo == null) {
      throw new RssException("eventInfo should not be null");
    }
    eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
    numFetchedSpills.getAndIncrement();

    if (srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
      eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());
    }

    lock.lock();
    try {
      /**
       * When fetch is complete for a spill, add it to completedInputs to ensure that it is
       * available for downstream processing. Final success will be claimed only when all spills are
       * downloaded from the source.
       */
      maybeInformInputReady(fetchedInput);

      // check if we downloaded all spills pertaining to this InputAttemptIdentifier
      if (eventInfo.isDone()) {
        adjustCompletedInputs(fetchedInput);
        shuffleInfoEventsMap.remove(srcAttemptIdentifier.getInputIdentifier());
      }
    } finally {
      lock.unlock();
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("eventInfo " + eventInfo.toString());
    }
  }

  private void reportFatalError(Throwable exception, String message) {
    LOG.error(message);
    inputContext.reportFailure(TaskFailureType.NON_FATAL, exception, message);
  }

  @Override
  public void fetchFailed(
      String host, InputAttemptIdentifier srcAttemptIdentifier, boolean connectFailed) {
    // NEWTEZ. Implement logic to report fetch failures after a threshold.
    // For now, reporting immediately.
    LOG.info(
        srcNameTrimmed
            + ": "
            + "Fetch failed for src: "
            + srcAttemptIdentifier
            + "InputIdentifier: "
            + srcAttemptIdentifier
            + ", connectFailed: "
            + connectFailed);
    failedShufflesCounter.increment(1);
    inputContext.notifyProgress();
    if (srcAttemptIdentifier == null) {
      reportFatalError(null, "Received fetchFailure for an unknown src (null)");
    } else {
      InputReadErrorEvent readError =
          InputReadErrorEvent.create(
              "Fetch failure while fetching from "
                  + TezRuntimeUtils.getTaskAttemptIdentifier(
                      inputContext.getSourceVertexName(),
                      srcAttemptIdentifier.getInputIdentifier(),
                      srcAttemptIdentifier.getAttemptNumber()),
              srcAttemptIdentifier.getInputIdentifier(),
              srcAttemptIdentifier.getAttemptNumber());
      if (maxTimeToWaitForReportMillis > 0) {
        reportLock.lock();
        try {
          failedEvents.merge(readError, 1, (a, b) -> a + b);
          reportCondition.signal();
        } finally {
          reportLock.unlock();
        }
      } else {
        List<Event> events = Lists.newArrayListWithCapacity(1);
        events.add(readError);
        inputContext.sendEvents(events);
      }
    }
  }
  // End of Methods from FetcherCallbackHandler

  @Override
  public void shutdown() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      // need to cleanup all FetchedInput (DiskFetchedInput, LocalDisFetchedInput), lockFile
      // As of now relying on job cleanup (when all directories would be cleared)
      LOG.info("{}: Thread interrupted. Need to cleanup the local dirs", srcNameTrimmed);
    }
    if (!isShutdown.getAndSet(true)) {
      // Shut down any pending fetchers
      LOG.info(
          "Shutting down pending fetchers on source"
              + srcNameTrimmed
              + ": "
              + rssRunningFetchers.size());
      lock.lock();
      try {
        wakeLoop.signal(); // signal the fetch-scheduler
        for (RssTezFetcherTask fetcher : rssRunningFetchers) {
          try {
            fetcher.shutdown(); // This could be parallelized.
          } catch (Exception e) {
            LOG.warn(
                "Error while stopping fetcher during shutdown. Ignoring and continuing. Message={}",
                e.getMessage());
          }
        }
      } finally {
        lock.unlock();
      }

      if (this.schedulerExecutor != null && !this.schedulerExecutor.isShutdown()) {
        this.schedulerExecutor.shutdownNow();
      }
      if (this.reporterExecutor != null && !this.reporterExecutor.isShutdown()) {
        this.reporterExecutor.shutdownNow();
      }
      if (this.fetcherExecutor != null && !this.fetcherExecutor.isShutdown()) {
        this.fetcherExecutor.shutdownNow(); // Interrupts all running fetchers.
      }
    }
  }

  /** @return true if all of the required inputs have been fetched. */
  public boolean isAllPartitionFetched() {
    lock.lock();
    try {
      if (!allRssPartition.containsAll(successRssPartitionSet)) {
        LOG.error(
            "Failed to check partition, all partition:{}, success partiton:{}",
            allRssPartition,
            successRssPartitionSet);
      }
      return isAllInputFetched();
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return the next available input, or null if there are no available inputs. This method will
   *     block if there are currently no available inputs, but more may become available.
   */
  @Override
  public FetchedInput getNextInput() throws InterruptedException {
    // Check for no additional inputs
    FetchedInput fetchedInput = null;
    if (completedInputs.peek() == null) {
      while (true) {
        fetchedInput = completedInputs.poll(2000, TimeUnit.MICROSECONDS);
        if (fetchedInput != null) {
          break;
        } else if (isAllPartitionFetched()) {
          fetchedInput = completedInputs.poll(100, TimeUnit.MICROSECONDS);
          LOG.info("GetNextInput, enter isAllPartitionFetched");
          break;
        }
        LOG.info("GetNextInput, out loop");
      }
    } else {
      fetchedInput = completedInputs.take();
    }

    if (fetchedInput instanceof NullFetchedInput) {
      LOG.info("getNextInput, NullFetchedInput is null:{}", fetchedInput);
      fetchedInput = null;
    }
    LOG.info("getNextInput, fetchedInput:{}", fetchedInput);
    return fetchedInput;
  }

  @Override
  public int getNumInputs() {
    return numInputs;
  }

  @Override
  public float getNumCompletedInputsFloat() {
    return numCompletedInputs.floatValue();
  }

  // End of methods for walking the available inputs

  /**
   * Fake input that is added to the completed input list in case an input does not have any data.
   */
  @VisibleForTesting
  static class NullFetchedInput extends FetchedInput {
    NullFetchedInput(InputAttemptIdentifier inputAttemptIdentifier) {
      super(inputAttemptIdentifier, null);
    }

    @Override
    public Type getType() {
      return Type.MEMORY;
    }

    @Override
    public long getSize() {
      return -1;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public InputStream getInputStream() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public void commit() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public void abort() throws IOException {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }

    @Override
    public void free() {
      throw new UnsupportedOperationException("Not supported for NullFetchedInput");
    }
  }

  private final AtomicInteger nextProgressLineEventCount = new AtomicInteger(0);

  private void logProgress() {
    int inputsDone = numCompletedInputs.get();
    if (inputsDone > nextProgressLineEventCount.get() || inputsDone == numInputs) {
      nextProgressLineEventCount.addAndGet(50);
      double mbs = (double) totalBytesShuffledTillNow / (1024 * 1024);
      long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

      double transferRate = mbs / secsSinceStart;
      LOG.info(
          "copy("
              + inputsDone
              + " (spillsFetched="
              + numFetchedSpills.get()
              + ") of "
              + numInputs
              + ". Transfer rate (CumulativeDataFetched/TimeSinceInputStarted)) "
              + mbpsFormat.format(transferRate)
              + " MB/s)");
    }
  }

  private class SchedulerFutureCallback implements FutureCallback<Void> {
    @Override
    public void onSuccess(Void result) {
      LOG.info("{}: Scheduler thread completed", srcNameTrimmed);
    }

    @Override
    public void onFailure(Throwable t) {
      if (isShutdown.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: Already shutdown. Ignoring error: ", srcNameTrimmed, t);
        }
      } else {
        LOG.error("{}: Scheduler failed with error: ", srcNameTrimmed, t);
        inputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Shuffle Scheduler Failed");
      }
    }
  }

  private class FetchFutureCallback implements FutureCallback<FetchResult> {

    private final RssTezFetcherTask fetcher;

    FetchFutureCallback(RssTezFetcherTask fetcher) {
      this.fetcher = fetcher;
    }

    private void doBookKeepingForFetcherComplete() {
      lock.lock();
      try {
        rssRunningFetchers.remove(fetcher);
        wakeLoop.signal();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void onSuccess(FetchResult result) {
      LOG.info(
          "FetchFutureCallback success, result:{}, partition:{}", result, fetcher.getPartitionId());
      fetcher.shutdown();
      if (isShutdown.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: Already shutdown. Ignoring event from fetcher", srcNameTrimmed);
        }
      } else {
        lock.lock();
        try {
          successRssPartitionSet.add(fetcher.getPartitionId());
          runningRssPartitionMap.remove(fetcher.getPartitionId());
          LOG.info(
              "FetchFutureCallback allRssPartition:{}, successRssPartitionSet:{}, runningRssPartitionMap:{}.",
              allRssPartition,
              successRssPartitionSet,
              runningRssPartitionMap);
          doBookKeepingForFetcherComplete();
        } finally {
          lock.unlock();
        }
      }
    }

    @Override
    public void onFailure(Throwable t) {
      // Unsuccessful - the fetcher may not have shutdown correctly. Try shutting it down.
      fetcher.shutdown();
      if (isShutdown.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: Already shutdown. Ignoring error from fetcher: ", srcNameTrimmed, t);
        }
      } else {
        LOG.error("{}: Fetcher failed with error: ", srcNameTrimmed, t);
        shuffleError = t;
        inputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Fetch failed");
        doBookKeepingForFetcherComplete();
      }
    }
  }
}
