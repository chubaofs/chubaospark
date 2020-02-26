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

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus

private[spark] class ChubaoShuffleWriter[K, V, C] (
  resolver: ChubaoShuffleBlockResolver,
  handle: BaseShuffleHandle[K, V, C],
  mapId: Int,
  context: TaskContext)
  extends ShuffleWriter[K, V]
  with Logging {

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    //TODO:
  }

  /** @inheritdoc */
  override def stop(success: Boolean): Option[MapStatus] = {
    //TODO:

    None
  }
}
