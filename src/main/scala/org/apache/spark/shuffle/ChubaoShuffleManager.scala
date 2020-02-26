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

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.sort._
import org.apache.spark.shuffle.sort.SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE

/**
  * Created by justice on 2020/2/17.
  */
private[spark] class ChubaoShuffleManager(conf: SparkConf)
  extends ShuffleManager
  with Logging {

  logInfo(s"initialize ChubaoShuffleManager ${ChubaoShuffleManager.version}")

  private val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  override lazy val shuffleBlockResolver = new ChubaoShuffleBlockResolver(conf.getAppId)

  override def registerShuffle[K, V, C](
     shuffleId: Int,
     numMaps: Int,
     dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (shouldBypassMergeSort(dependency)) {
      logInfo("register ChubaoBypassMergeSortShuffleHandle")
      new ChubaoBypassMergeSortShuffleHandle[K, V](shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else if (useSerializedShuffle(dependency)) {
      logInfo("register ChubaoSerializedShuffleHandle")
      new ChubaoSerializedShuffleHandle[K, V](shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      logInfo("register BaseShuffleHandle")
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps).foreach { mapId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  override def getWriter[K, V](
    handle: ShuffleHandle,
    mapId: Int,
    context: TaskContext): ShuffleWriter[K, V] = {
    logDebug(s"get writer for shuffle ${handle.shuffleId} mapper $mapId.")

    numMapsForShuffle.putIfAbsent(handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)

    val env = SparkEnv.get
    handle match {
      case unsafeShuffleHandle: ChubaoSerializedShuffleHandle[K@unchecked, V@unchecked] =>
        new UnsafeShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf)
      case bypassShuffleHandle: ChubaoBypassMergeSortShuffleHandle[K@unchecked, V@unchecked] =>
        new ChubaoBypassMergeSortShuffleWriter(shuffleBlockResolver, bypassShuffleHandle, mapId, context)
      case baseHandle: BaseShuffleHandle[K@unchecked, V@unchecked, _] =>
        new ChubaoShuffleWriter(shuffleBlockResolver, baseHandle, mapId, context)
    }

  }

  override def getReader[K, C](
    handle: ShuffleHandle,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext): ShuffleReader[K, C] = {
    new ChubaoShuffleReader(shuffleBlockResolver,
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
      startPartition,
      endPartition,
      context)
  }

  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }

  private def shouldBypassMergeSort(dep: ShuffleDependency[_, _, _]): Boolean = {
    if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      false
    } else {
      val bypassMergeThreshold: Int = conf.get(ChubaoShuffleOpts.bypassSortThreshold)
      logInfo(s"partitions ${dep.partitioner.numPartitions}, " +
        s"bypass merge threshold $bypassMergeThreshold")
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }

  private def useSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val optionKey = "spark.shuffle.chubao.useBaseShuffle"
    val useBaseShuffle = conf.getBoolean(optionKey, defaultValue = false)
    !useBaseShuffle && canUseSerializedShuffle(dependency)
  }

  private def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val shuffleId = dependency.shuffleId
    val numPartitions = dependency.partitioner.numPartitions
    if (!dependency.serializer.supportsRelocationOfSerializedObjects) {
      log.debug(s"Can't use serialized shuffle for shuffle $shuffleId because the serializer, " +
        s"${dependency.serializer.getClass.getName}, does not support object relocation")
      false
    } else if (dependency.aggregator.isDefined) {
      log.debug(
        s"Can't use serialized shuffle for shuffle $shuffleId because an aggregator is defined")
      false
    } else if (numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
      log.debug(s"Can't use serialized shuffle for shuffle $shuffleId because it has more than " +
        s"$MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE partitions")
      false
    } else {
      log.debug(s"Can use serialized shuffle for shuffle $shuffleId")
      true
    }
  }
}

private[spark] class ChubaoBypassMergeSortShuffleHandle[K, V](
   shuffleId: Int,
   numMaps: Int,
   dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

private[spark] class ChubaoSerializedShuffleHandle[K, V](
  shuffleId: Int,
  numMaps: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}


object ChubaoShuffleManager {
  def version: String = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("chubao-spark-shuffle.properties"))
    properties.getProperty("chubao-spark-shuffle.version")
  }
}
