/*
 * Copyright (c) 2020 the Chubao Authors.
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
 *
 */

package org.apache.spark.shuffle

import java.io.File

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.ShuffleBlockId

/**
  * Created by justice on 2020/2/17.
  */
private[spark] class ChubaoShuffleBlockResolver(appId: String)
  extends ShuffleBlockResolver
  with Logging {

  val NOOP_REDUCE_ID = 0

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer =
    throw new UnsupportedOperationException("UnsupportedOperation.")

  def getDataFile(shuffleId: Int, mapId: Int): File = {
    //TODO:
  }

  def getDataTmpFile(shuffleId: Int, mapId: Int): File = {
    //TODO:
  }

  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    //TODO:
  }

  /**
    *
    * @param shuffleId
    * @param mapId
    * @param lengths
    * @param dataTmp
    */
  def writeIndexFileAndCommit(shuffleId: Int, mapId: Int, lengths: Array[Long], dataTmp: File): Unit = {
    //TODO:

  }


  override def stop(): Unit = {

  }

}

private[spark] object ChubaoShuffleBlockResolver {
}

