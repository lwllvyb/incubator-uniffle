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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.log.TestLoggerExtension;
import org.apache.uniffle.common.log.TestLoggerParamResolver;
import org.apache.uniffle.common.storage.StorageInfo;
import org.apache.uniffle.common.storage.StorageMedia;
import org.apache.uniffle.common.storage.StorageStatus;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.ShuffleTaskInfo;
import org.apache.uniffle.storage.common.DefaultStorageMediaProvider;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static uk.org.webcompere.systemstubs.SystemStubs.withEnvironmentVariables;

/** The class is to test the {@link LocalStorageManager} */
@ExtendWith(TestLoggerExtension.class)
@ExtendWith(TestLoggerParamResolver.class)
public class LocalStorageManagerTest {

  @BeforeAll
  public static void prepare() {
    ShuffleServerMetrics.register();
  }

  @AfterAll
  public static void clear() {
    ShuffleServerMetrics.clear();
  }

  private ShuffleDataFlushEvent toDataFlushEvent(String appId, int shuffleId, int startPartition) {
    return new ShuffleDataFlushEvent(
        1, // event id
        appId, // appId
        shuffleId, // shuffle id
        startPartition, // startPartition
        1, // endPartition
        1, // size
        new ArrayList<ShufflePartitionedBlock>(), // shuffleBlocks
        null, // valid
        null // shuffleBuffer
        );
  }

  @Test
  public void testStorageSelectionWhenReachingHighWatermark() {
    String[] storagePaths = {"/tmp/rss-data1", "/tmp/rss-data2", "/tmp/rss-data3"};

    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storagePaths));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(),
        org.apache.uniffle.storage.util.StorageType.LOCALFILE.name());

    LocalStorageManager localStorageManager = new LocalStorageManager(conf);

    String appId = "testStorageSelectionWhenReachingHighWatermark";
    ShuffleDataFlushEvent dataFlushEvent = toDataFlushEvent(appId, 1, 1);
    Storage storage1 = localStorageManager.selectStorage(dataFlushEvent);

    ((LocalStorage) storage1).getMetaData().setSize(999);
    localStorageManager = new LocalStorageManager(conf);
    Storage storage2 = localStorageManager.selectStorage(dataFlushEvent);

    assertNotEquals(storage1, storage2);
  }

  @Test
  public void testStorageSelection() {
    String[] storagePaths = {"/tmp/rss-data1", "/tmp/rss-data2", "/tmp/rss-data3"};

    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storagePaths));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(),
        org.apache.uniffle.storage.util.StorageType.LOCALFILE.name());
    LocalStorageManager localStorageManager = new LocalStorageManager(conf);

    List<LocalStorage> storages = localStorageManager.getStorages();
    assertNotNull(storages);

    String appId = "testCorruptedStorageApp";

    // case1: no corrupted storage, flush and read event of the same appId and shuffleId and
    // startPartition
    // will always get the same storage
    ShuffleDataFlushEvent dataFlushEvent1 = toDataFlushEvent(appId, 1, 1);
    Storage storage1 = localStorageManager.selectStorage(dataFlushEvent1);

    ShuffleDataFlushEvent dataFlushEvent2 = toDataFlushEvent(appId, 1, 1);
    Storage storage2 = localStorageManager.selectStorage(dataFlushEvent2);

    ShuffleDataReadEvent dataReadEvent = new ShuffleDataReadEvent(appId, 1, 1, 1);
    Storage storage3 = localStorageManager.selectStorage(dataReadEvent);
    assertEquals(storage1, storage2);
    assertEquals(storage1, storage3);

    // case2: one storage is corrupted, and it will switch to other storage at the first time of
    // writing
    // event of (appId, shuffleId, startPartition)
    ((LocalStorage) storage1).markCorrupted();
    Storage storage4 = localStorageManager.selectStorage(dataFlushEvent1);
    assertNotEquals(storage4.getStoragePath(), storage1.getStoragePath());
    assertEquals(localStorageManager.selectStorage(dataReadEvent), storage4);

    // case3: one storage is corrupted when it happened after the original event has been written,
    // so it will switch to another storage for write and read event.
    LocalStorage mockedStorage = spy((LocalStorage) storage4);
    when(mockedStorage.containsWriteHandler(appId, 1, 1)).thenReturn(true);
    Storage storage5 = localStorageManager.selectStorage(dataFlushEvent1);
    Storage storage6 = localStorageManager.selectStorage(dataReadEvent);
    assertNotEquals(storage1, storage5);
    assertEquals(storage4, storage5);
    assertEquals(storage5, storage6);

    // case4: one storage is corrupted when it happened after the original event has been written,
    // but before reading this partition, another storage corrupted, it still could read the
    // original data.
    Storage storage7 = localStorageManager.selectStorage(dataFlushEvent1);
    Storage restStorage =
        storages.stream().filter(x -> !x.isCorrupted() && x != storage7).findFirst().get();
    ((LocalStorage) restStorage).markCorrupted();
    Storage storage8 = localStorageManager.selectStorage(dataReadEvent);
    assertEquals(storage7, storage8);

    // make all storage corrupted
    ((LocalStorage) localStorageManager.selectStorage(dataFlushEvent1)).markCorrupted();
    ShuffleDataFlushEvent dataFlushEvent3 = toDataFlushEvent(appId, 1, 2);
    assertNull(localStorageManager.selectStorage(dataFlushEvent3));
  }

  @Test
  public void testInitLocalStorageManager() {
    String[] storagePaths = {"/tmp/rssdata", "/tmp/rssdata2"};

    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storagePaths));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(),
        org.apache.uniffle.storage.util.StorageType.LOCALFILE.name());
    LocalStorageManager localStorageManager = new LocalStorageManager(conf);

    List<LocalStorage> storages = localStorageManager.getStorages();
    assertNotNull(storages);
    assertEquals(storages.size(), storagePaths.length);
    for (int i = 0; i < storagePaths.length; i++) {
      assertEquals(storagePaths[i], storages.get(i).getBasePath());
    }
  }

  @Test
  public void testInitializeLocalStorage() throws IOException {
    final File storageBaseDir1 = Files.createTempDirectory("rss-data-1").toFile();
    final File storageBaseDir2 = Files.createTempDirectory("rss-data-2").toFile();
    final File rootRestrictedDir1 = new File("/proc/rss-data-mock-restricted-dir-1");
    final File rootRestrictedDir2 = new File("/proc/rss-data-mock-restricted-dir-2");

    // case1: when no candidates, it should throw exception.
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 2000L);
    conf.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH,
        Arrays.asList(rootRestrictedDir1.getAbsolutePath(), rootRestrictedDir2.getAbsolutePath()));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setLong(ShuffleServerConf.LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER, 1);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    try {
      LocalStorageManager localStorageManager = new LocalStorageManager(conf);
      fail();
    } catch (Exception e) {
      // ignore
    }

    // case2: when candidates exist, it should initialize successfully.
    conf.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH,
        Arrays.asList(storageBaseDir1.getAbsolutePath(), rootRestrictedDir1.getAbsolutePath()));
    LocalStorageManager localStorageManager = new LocalStorageManager(conf);
    assertEquals(1, localStorageManager.getStorages().size());

    // case3: all ok
    conf.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH,
        Arrays.asList(storageBaseDir1.getAbsolutePath(), storageBaseDir2.getAbsolutePath()));
    localStorageManager = new LocalStorageManager(conf);
    assertEquals(2, localStorageManager.getStorages().size());

    // case4: after https://github.com/apache/incubator-uniffle/pull/616
    // dirs will be created automatically if they do not exist
    FileUtils.deleteQuietly(storageBaseDir1);
    FileUtils.deleteQuietly(storageBaseDir2);
    conf.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH,
        Arrays.asList(storageBaseDir1.getAbsolutePath(), storageBaseDir2.getAbsolutePath()));
    localStorageManager = new LocalStorageManager(conf);
    assertEquals(2, localStorageManager.getStorages().size());

    // case5: only have 1 candidate, but exceed the number of
    // rss.server.localstorage.initialize.max.fail.number
    conf.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH,
        Arrays.asList(storageBaseDir1.getAbsolutePath(), rootRestrictedDir1.getAbsolutePath()));
    conf.setLong(ShuffleServerConf.LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER, 0L);
    try {
      localStorageManager = new LocalStorageManager(conf);
      fail();
    } catch (Exception e) {
      // ignore
    }

    // case6: if failed=2, but lower than rss.server.localstorage.initialize.max.fail.number,
    // it should fail too
    conf.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH,
        Arrays.asList(rootRestrictedDir1.getAbsolutePath(), rootRestrictedDir2.getAbsolutePath()));
    conf.setLong(ShuffleServerConf.LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER, 10L);
    try {
      localStorageManager = new LocalStorageManager(conf);
      fail();
    } catch (Exception e) {
      // ignore
    }

    // clear temp dirs
    FileUtils.deleteQuietly(storageBaseDir1);
    FileUtils.deleteQuietly(storageBaseDir2);
  }

  @Test
  public void testGetLocalStorageInfo() throws IOException {
    Path testBaseDir = Files.createTempDirectory("rss-test");
    final Path storageBaseDir1 = Files.createDirectory(testBaseDir.resolve("rss-data-1"));
    final Path storageBaseDir2 = Files.createDirectory(testBaseDir.resolve("rss-data-2"));
    final Path storageBaseDir3 = Files.createDirectory(testBaseDir.resolve("rss-data-3"));

    String[] storagePaths = {
      storageBaseDir1.toString(), storageBaseDir2.toString(), storageBaseDir3.toString()
    };
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storagePaths));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(),
        org.apache.uniffle.storage.util.StorageType.LOCALFILE.name());
    conf.setDouble(ShuffleServerConf.HEALTH_STORAGE_MAX_USAGE_PERCENTAGE, 100.0D);

    // Write one file to each storage dir
    final LocalStorageManager localStorageManager = new LocalStorageManager(conf);
    Files.write(storageBaseDir1.resolve("partition1.data"), new byte[] {0x1});
    Files.write(storageBaseDir2.resolve("partition2.data"), new byte[] {0x2});
    Files.write(storageBaseDir3.resolve("partition3.data"), new byte[] {0x3});

    assertTrue(localStorageManager.getStorageChecker().checkIsHealthy(), "should be healthy");

    Map<String, StorageInfo> storageInfo = localStorageManager.getStorageInfo();
    assertEquals(1, storageInfo.size());

    String mountPoint = Files.getFileStore(testBaseDir).name();
    assertNotNull(storageInfo.get(mountPoint));

    // Use production logic to get expected media type
    StorageMedia expected =
        new DefaultStorageMediaProvider().getStorageMediaFor(testBaseDir.toString());

    // Assert media type from storage manager matches production logic
    assertEquals(expected, storageInfo.get(mountPoint).getType());

    // Assert status and used bytes
    assertEquals(StorageStatus.NORMAL, storageInfo.get(mountPoint).getStatus());
    assertEquals(3L, storageInfo.get(mountPoint).getUsedBytes());
  }

  @Test
  public void testEnvStorageTypeProvider() throws Exception {
    String[] storagePaths = {"/tmp/rss-data1"};

    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storagePaths));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(),
        org.apache.uniffle.storage.util.StorageType.LOCALFILE.name());
    conf.set(ShuffleServerConf.STORAGE_MEDIA_PROVIDER_ENV_KEY, "env_key");
    withEnvironmentVariables("env_key", "{\"/tmp\": \"ssd\"}")
        .execute(
            () -> {
              LocalStorageManager localStorageManager = new LocalStorageManager(conf);
              Map<String, StorageInfo> storageInfo = localStorageManager.getStorageInfo();
              assertEquals(1, storageInfo.size());
              String mountPoint = Files.getFileStore(new File("/tmp").toPath()).name();
              assertNotNull(storageInfo.get(mountPoint));
              // by default, it should report HDD as local storage type
              assertEquals(StorageMedia.SSD, storageInfo.get(mountPoint).getType());
            });
  }

  @Test
  public void testNewAppWhileCheckLeak(ExtensionContext context) {
    String[] storagePaths = {"/tmp/rss-data1"};

    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storagePaths));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(),
        org.apache.uniffle.storage.util.StorageType.LOCALFILE.name());
    LocalStorageManager localStorageManager = new LocalStorageManager(conf);

    List<LocalStorage> storages = localStorageManager.getStorages();
    assertNotNull(storages);

    // test normal case
    Map<String, ShuffleTaskInfo> shuffleTaskInfos = JavaUtils.newConcurrentMap();
    shuffleTaskInfos.put("app0", new ShuffleTaskInfo("app0"));
    shuffleTaskInfos.put("app1", new ShuffleTaskInfo("app1"));
    shuffleTaskInfos.put("app2", new ShuffleTaskInfo("app2"));
    localStorageManager.checkAndClearLeakedShuffleData(shuffleTaskInfos::keySet);

    // test race condition case, app 3 is new app
    shuffleTaskInfos.put("3", new ShuffleTaskInfo("app3"));
    LocalStorage mockLocalStorage = mock(LocalStorage.class);
    when(mockLocalStorage.getAppIds()).thenReturn(Collections.singleton("app3"));
    storages.add(mockLocalStorage);
    localStorageManager.checkAndClearLeakedShuffleData(shuffleTaskInfos::keySet);

    TestLoggerExtension testLogger = TestLoggerExtension.getTestLogger(context);
    assertTrue(testLogger.wasLogged("Delete shuffle data for appId\\[app3\\]"));
    System.out.println();
  }
}
