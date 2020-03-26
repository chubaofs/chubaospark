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

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.{BlockManagerId, ChubaoFSStorageManager, ShuffleBlockId, ShuffleIndexBlockId}

/**
  * Created by justice on 2020/2/17.
  */
private[spark] class ChubaoShuffleBlockResolver(conf: SparkConf)
  extends ShuffleBlockResolver
  with Logging {

  def getAppId: String = conf.getAppId

  val NOOP_REDUCE_ID = 0

  val blockManagerId: BlockManagerId = BlockManagerId(getAppId, "cfs", 666, None)

  private lazy val storageManger = new ChubaoFSStorageManager(conf)

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer =
    throw new UnsupportedOperationException("UnsupportedOperation.")

  def getDataFile(shuffleId: Int, mapId: Int): File =
    storageManger.getDataFile(shuffleId, mapId, NOOP_REDUCE_ID)


  private def getIndexFile(shuffleId: Int, mapId: Int): File =
    storageManger.getIndexFile(shuffleId, mapId, NOOP_REDUCE_ID)


  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
  }

  /**
    *
    * @param shuffleId
    * @param mapId
    * @param lengths
    * @param dataTmp
    */
  def writeIndexFileAndCommit(shuffleId: Int, mapId: Int, lengths: Array[Long], dataTmp: File): Unit = {

  }

  def getBlockData(blockId: BlockId): Option[BlockDataStreamInfo] = {
    if (blockId.isShuffle) {
      val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
      if (shuffleTypeManager.isHashBasedShuffle(shuffleBlockId)) {
        readHashBasedPartition(shuffleBlockId)
      } else {
        readPartitionByIndex(shuffleBlockId)
      }
    } else {
      logError(s"only works for shuffle block id, current block id: $blockId")
      None
    }
  }

  private def readHashBasedPartition(shuffleBlockId: ShuffleBlockId) = {
    val dataFile = getDataFile(
      shuffleBlockId.shuffleId,
      shuffleBlockId.mapId,
      shuffleBlockId.reduceId)

    if (traceDataChecksum) logPartitionMd5(dataFile)

    val stream = dataFile.makeBufferedInputStream()
    Some(BlockDataStreamInfo(stream, dataFile.getSize))
  }

  private def readPartitionByIndex(shuffleBlockId: ShuffleBlockId) = {
    val partitionLoc = shuffleCache.getPartitionLoc(shuffleBlockId)
    partitionLoc match {
      case Some(partition) =>
        val dataFile = getDataFile(shuffleBlockId)

        if (traceDataChecksum) logPartitionMd5(dataFile, partition)

        val stream = dataFile.makeBufferedInputStreamWithin(partition)
        Some(BlockDataStreamInfo(stream, partition.length))
      case None => None
    }
  }

  private def logPartitionMd5(dataFile: ShuffleFile): Some[InputStream] =
    logPartitionMd5(dataFile, 0, dataFile.getSize)

  private def logPartitionMd5(
                               dataFile: ShuffleFile, partitionLoc: PartitionLoc): Some[InputStream] =
    logPartitionMd5(dataFile, partitionLoc.start, partitionLoc.end)

  private def logPartitionMd5(
                               dataFile: ShuffleFile,
                               offset: Long,
                               nextOffset: Long): Some[InputStream] = {
    logDebug(s"read partition from $offset to $nextOffset " +
      s"in ${dataFile.getPath} size ${dataFile.getSize}.")
    SplashUtils.withResources {
      val stream = dataFile.makeBufferedInputStreamWithin(offset, nextOffset)
      new BufferedInputStream(stream)
    } { is =>
      val buf = new Array[Byte]((nextOffset - offset).asInstanceOf[ShuffleId])
      is.read(buf)
      val md: MessageDigest = MessageDigest.getInstance("MD5")
      val theDigest: Array[Byte] = md.digest(buf)
      val str = theDigest.map("%02X" format _).mkString
      logDebug(s"md5 for ${dataFile.getPath} offset $offset, length ${buf.length}: $str")
      Some(is)
    }
  }

  override def stop(): Unit = {
  }

}

private[spark] object ChubaoShuffleBlockResolver {
  val NOOP_REDUCE_ID = 0
}

