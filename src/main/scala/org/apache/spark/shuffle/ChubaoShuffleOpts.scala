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

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.network.util.ByteUnit

object ChubaoShuffleOpts {

  lazy val bypassSortThreshold: ConfigEntry[Int] =
    createIfNotExists("spark.shuffle.sort.bypassMergeThreshold", builder => {
      builder.doc("Use bypass merge sort shuffle writer if partition is lower than this")
        .intConf
        .createWithDefault(200)
    })

  private def createIfNotExists[T](
    optionKey: String,
    f: ConfigBuilder => ConfigEntry[T]): ConfigEntry[T] = {
      val existingEntry: ConfigEntry[_] = ConfigEntry.findEntry(optionKey)
      if (existingEntry != null) {
        existingEntry.asInstanceOf[ConfigEntry[T]]
      } else {
        f(ConfigBuilder(optionKey))
      }
  }

  lazy val shuffleRootFolder: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.root.folder")
      .doc("location of the shuffle root folder")
      .stringConf
      .createWithDefault("cfs://mnt/cfs")
}
