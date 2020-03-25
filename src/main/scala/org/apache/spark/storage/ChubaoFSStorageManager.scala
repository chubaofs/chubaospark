/*
 * Copyright (c) 2020. the Chubao Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.spark.storage

import java.io.{File, IOException}
import java.nio.file.{Path, Paths}

import com.jd.chubao.spark.shuffle.{ShuffleFile, ShuffleTmpFile}
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ChubaoShuffleOpts
import org.apache.spark.util.Utils

/**
  * Created by justice on 2020/3/5.
  */
private[spark] class ChubaoFSStorageManager(conf: SparkConf) extends Logging {

  val cfsPrefix = "cfs://"

  lazy val appId = conf.getAppId

  lazy val shuffleRootFolder = getRootFolder()

  def getRootFolder(): String = {
    val rootFolder = conf.get(ChubaoShuffleOpts.shuffleRootFolder)
    if (!rootFolder.startsWith(cfsPrefix)) {
      logWarning(s"${ChubaoShuffleOpts.shuffleRootFolder} must start with cfs://")
      rootFolder
    } else {
      rootFolder.substring(cfsPrefix.length()-1)
    }
  }

  def getFile(filename: String): File = {
    new File(filename)
  }

  def getDataFile(shuffleId: Int, mapId: Int, reduceId: Int): File =
    getFile(getShuffleDataFilename(shuffleId, mapId, reduceId))


  def getIndexFile(shuffleId: Int, mapId: Int, reduceId: Int): File =
    getFile(getShuffleIndexFilename(shuffleId, mapId, reduceId))


  def getShuffleDataFilename(shuffleId: Int, mapId: Int, reduceId: Int): String =
    Paths.get(
      s"${shuffleRootFolder}",
      s"${appId}",
      s"shuffle_$shuffleId",
      s"shuffle_${shuffleId}_${mapId}_${reduceId}.data"
    ).toString


  def getShuffleIndexFilename(shuffleId: Int, mapId: Int, reduceId: Int): String =
    Paths.get(
      s"${shuffleRootFolder}",
      $"${appId}",
      s"shuffle_$shuffleId",
      s"shuffle_${shuffleId}_${mapId}_${reduceId}.index"
    ).toString


}


object ChubaoFSStorageManager extends Logging {


}