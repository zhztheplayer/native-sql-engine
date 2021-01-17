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

package org.apache.spark.sql.execution

import java.io._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.intel.oap.expression.ConverterUtils
import com.intel.oap.vectorized.{ArrowWritableColumnVector, BatchIterator, ExpressionEvaluator, SerializableObject}
import org.apache.spark.util.{KnownSizeEstimation, Utils}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import java.util.concurrent.atomic.AtomicBoolean

import sun.misc.Cleaner

class ColumnarHashedRelation(
    var hashRelationObj: SerializableObject,
    var arrowColumnarBatch: Array[ColumnarBatch],
    var arrowColumnarBatchSize: Int)
    extends Externalizable
    with KryoSerializable
    with KnownSizeEstimation {
  val closed: AtomicBoolean = new AtomicBoolean()

  var cleaner: Cleaner = createCleaner

  private def createCleaner = {
    null
//    Cleaner.create(this, new Runnable {
//      override def run(): Unit = {
//        close()
//      }
//    })
  }

  def asReadOnlyCopy(): ColumnarHashedRelation = {
    //new ColumnarHashedRelation(hashRelationObj, arrowColumnarBatch, arrowColumnarBatchSize)
    this
  }

  override def estimatedSize: Long = 0

  def close(): Unit = {
    if (closed.getAndSet(true)) {
      throw new IllegalStateException("ColumnarhashedRelation already closed")
    }
    println("[HONGZE DEBUG] Closing ColumnarHashedRelation")
    hashRelationObj.close
    arrowColumnarBatch.foreach(_.close)
  }
//
//  override def finalize(): Unit = {
//    if (!closed.getAndSet(true)) {
//      println("[HONGZE DEBUG] Closing ColumnarHashedRelation")
//      hashRelationObj.close
//      arrowColumnarBatch.foreach(_.close)
//    }
//  }

  override def writeExternal(out: ObjectOutput): Unit = {
    if (closed.get()) return
    out.writeObject(hashRelationObj)
    val rawArrowData = ConverterUtils.convertToNetty(arrowColumnarBatch)
    out.writeObject(rawArrowData)
  }

  override def write(kryo: Kryo, out: Output): Unit = {
    if (closed.get()) return
    kryo.writeObject(out, hashRelationObj)
    val rawArrowData = ConverterUtils.convertToNetty(arrowColumnarBatch)
    kryo.writeObject(out, rawArrowData)
  }

  override def readExternal(in: ObjectInput): Unit = {
    if (cleaner == null) {
      cleaner = createCleaner
    }
    hashRelationObj = in.readObject().asInstanceOf[SerializableObject]
    val rawArrowData = in.readObject().asInstanceOf[Array[Byte]]
    arrowColumnarBatchSize = rawArrowData.length
    arrowColumnarBatch =
      ConverterUtils.convertFromNetty(null, new ByteArrayInputStream(rawArrowData)).toArray
    // retain all cols
    /*arrowColumnarBatch.foreach(cb => {
      (0 until cb.numCols).toList.foreach(i =>
        cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
    })*/
  }

  override def read(kryo: Kryo, in: Input): Unit = {
    if (cleaner == null) {
      cleaner = createCleaner
    }
    hashRelationObj =
      kryo.readObject(in, classOf[SerializableObject]).asInstanceOf[SerializableObject]
    val rawArrowData = kryo.readObject(in, classOf[Array[Byte]]).asInstanceOf[Array[Byte]]
    arrowColumnarBatchSize = rawArrowData.length
    arrowColumnarBatch =
      ConverterUtils.convertFromNetty(null, new ByteArrayInputStream(rawArrowData)).toArray
    // retain all cols
    /*arrowColumnarBatch.foreach(cb => {
      (0 until cb.numCols).toList.foreach(i =>
        cb.column(i).asInstanceOf[ArrowWr:w
        itableColumnVector].retain())
    })*/
  }

  def size(): Int = {
    hashRelationObj.total_size + arrowColumnarBatchSize
  }

  def getColumnarBatchAsIter: Iterator[ColumnarBatch] = {
    if (closed.get())
      throw new InvalidObjectException(
        s"can't getColumnarBatchAsIter from a deleted ColumnarHashedRelation.")
    new Iterator[ColumnarBatch] {
      var idx = 0
      val total_len = arrowColumnarBatch.length
      override def hasNext: Boolean = idx < total_len
      override def next(): ColumnarBatch = {
        val tmp_idx = idx
        idx += 1
        val cb = arrowColumnarBatch(tmp_idx)
        // retain all cols
        (0 until cb.numCols).toList.foreach(i =>
          cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
        cb
      }
    }
  }

}
