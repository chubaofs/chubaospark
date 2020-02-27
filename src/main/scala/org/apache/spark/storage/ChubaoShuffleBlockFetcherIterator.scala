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

import java.io.{ByteArrayInputStream, File, IOException, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue

import io.netty.buffer.{ByteBufInputStream, Unpooled}
import javax.annotation.concurrent.GuardedBy
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}
import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, ShuffleClient, TempFileManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBufferOutputStream
/**
  * Created by justice on 2020/2/27.
  */

private[spark]
final class ChubaoShuffleBlockFetcherIterator(
   shuffleId: Int,
   startPartition: Int,
   endPartition: Int,
   context: TaskContext,
   streamWrapper: (BlockId, InputStream) => InputStream)
  extends Iterator[(String, InputStream)] with Logging {

  override def hasNext: Boolean = partitionInputStream.available() > 0

  /**
    * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
    * underlying each InputStream will be freed by the cleanup() method registered with the
    * TaskCompletionListener. However, callers should close() these InputStreams
    * as soon as they are no longer needed, in order to release memory as early as possible.
    *
    * Throws a FetchFailedException if the next block could not be fetched.
    */
  override def next(): (String, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    //TODO: next

  }
}

private class ChubaoBufferReleasingInputStream(
  private val delegate: InputStream,
  private val iterator: ChubaoShuffleBlockFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

