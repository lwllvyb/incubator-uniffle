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

package org.apache.uniffle.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.examples.SimpleSessionExample;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.exception.RssException;

public class TezSimpleSessionExampleTest extends TezIntegrationTestBase {

  private final String inputPath = "simple_session_input";
  private final String outputPath = "simple_session_output";
  private final List<String> wordTable =
      Lists.newArrayList(
          "apple", "banana", "fruit", "cherry", "Chinese", "America", "Japan", "tomato");

  @BeforeAll
  public static void setupServers() throws Exception {
    TezIntegrationTestBase.setupServers(null);
  }

  @Test
  public void simpleSessionExampleTest() throws Exception {
    generateInputFile();
    run();
  }

  @Override
  public void updateCommonConfiguration(Configuration appConf) throws Exception {
    super.updateCommonConfiguration(appConf);
    appConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
  }

  @Override
  public void updateRssConfiguration(Configuration appConf, ClientType clientType) {
    super.updateRssConfiguration(appConf, clientType);
    appConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
  }

  private void generateInputFile() throws Exception {
    for (int i = 0; i < 3; i++) {
      generateInputFile(inputPath + "." + i);
    }
  }

  private void generateInputFile(String inputPath) throws Exception {
    // For ordered word count, the key of last ordered sorter is the summation of word, the value is
    // the word. So it means this key may not be unique. Because Sorter can only make sure key is
    // sorted, so the second column (word column) may be not sorted.
    // To keep pace with verifyResults, here make sure summation of word is unique number.
    FSDataOutputStream outputStream = fs.create(new Path(inputPath));
    Random random = new Random();
    Set<Integer> used = new HashSet<>();
    List<String> outputList = new ArrayList<>();
    int index = 0;
    while (index < wordTable.size()) {
      int summation = random.nextInt(50);
      if (used.contains(summation)) {
        continue;
      }
      used.add(summation);
      for (int i = 0; i < summation; i++) {
        outputList.add(wordTable.get(index));
      }
      index++;
    }
    Collections.shuffle(outputList);
    for (String word : outputList) {
      outputStream.writeBytes(word + "\n");
    }
    outputStream.close();
  }

  @Override
  public Tool getTestTool() {
    return new SimpleSessionExample();
  }

  @Override
  public String[] getTestArgs(String uniqueOutputName) {
    return new String[] {
      inputPath + ".0," + inputPath + ".1," + inputPath + ".2",
      outputPath
          + "/"
          + uniqueOutputName
          + ".0"
          + ","
          + outputPath
          + "/"
          + uniqueOutputName
          + ".1"
          + ","
          + outputPath
          + "/"
          + uniqueOutputName
          + ".2",
      "2"
    };
  }

  @Override
  public String getOutputDir(String uniqueOutputName) {
    return outputPath
        + "/"
        + uniqueOutputName
        + ".0"
        + ","
        + outputPath
        + "/"
        + uniqueOutputName
        + ".1"
        + ","
        + outputPath
        + "/"
        + uniqueOutputName
        + ".2";
  }

  @Override
  public void verifyResults(String originPath, String rssPath) throws Exception {
    String[] originPaths = originPath.split(",");
    String[] rssPaths = rssPath.split(",");
    if (originPaths.length != rssPaths.length) {
      throw new RssException("The length of paths is mismatched!");
    }
    for (int i = 0; i < originPaths.length; i++) {
      verifyResultEqual(originPaths[i], rssPaths[i]);
    }
  }
}
