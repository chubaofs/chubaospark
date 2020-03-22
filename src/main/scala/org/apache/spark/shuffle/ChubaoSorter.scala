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

package org.apache.spark.shuffle

import java.io._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.storage.BlockId
import org.apache.spark.util.collection._

/**
  * Created by justice on 2020/2/21.
  */
private[spark] class ChubaoSorter[K, V, C](
    context: TaskContext,
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Serializer = SparkEnv.get.serializer)
  extends Spillable[WritablePartitionedPairCollection[K, C]](context.taskMemoryManager())
    with Logging {

  private val conf = SparkEnv.get.conf
  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)

  override protected def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    //TODO:
  }

  override protected def forceSpill(): Boolean = {
    //TODO:

    false
  }

  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    //TODO:

  }

  def writePartitionedFile(
    blockId: BlockId,
    outputFile: File): Array[Long] = {

    val lengths = new Array[Long](numPartitions)

    //TODO:

    lengths
  }

  def stop(): Unit = {
    //TODO:
  }
}

