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

import org.apache.spark.internal.{Logging, config}
import org.apache.spark.storage.{BlockId, ChubaoShuffleBlockFetcherIterator}
import org.apache.spark.shuffle._
import org.apache.spark.util.CompletionIterator
import org.apache.spark.{InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}

private[spark] class ChubaoShuffleReader[K, C](
    resolver: ChubaoShuffleBlockResolver,
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C]
  with Logging {

  private val dep = handle.dependency

  override def read(): Iterator[Product2[K, C]] = {
    val shuffleBlocks = mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startPartition, endPartition)
      .flatMap(_._2)
    readShuffleBlocks(shuffleBlocks)
  }

  def readShuffleBlocks(shuffleBlocks: Iterator[(BlockId, Long)]): Iterator[Product2[K, C]] = {
    val taskMetrics = context.taskMetrics()
    val serializer = dep.serializer.newInstance()


    val nonEmptyBlocks = shuffleBlocks.filter(_._2 > 0).map(_._1)
    val fetcherIterator = ChubaoShuffleFetcherIterator(resolver, nonEmptyBlocks)


  }

}
