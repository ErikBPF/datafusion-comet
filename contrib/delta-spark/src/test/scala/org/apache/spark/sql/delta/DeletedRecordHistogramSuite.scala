/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.delta

import java.nio.{ByteBuffer, ByteOrder}

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.delta.stats.DeletedRecordCountsHistogramUtils.DeletedRecordCountsHistogramAgg
import org.apache.spark.sql.types.{ArrayType, LongType}

/**
 * Spark/Delta reference-state contract, without Comet execution or state interoperability claims.
 * The Delta package permits access to the private[delta] helper in Delta 3.3 and 4.0.
 */
class DeletedRecordHistogramSuite extends AnyFunSuite {
  private def aggregate =
    DeletedRecordCountsHistogramAgg(BoundReference(0, LongType, nullable = true))

  test("empty and null-only histograms keep ten zero bins and independent buffers") {
    val agg = aggregate
    assert(agg.dataType == ArrayType(LongType))
    assert(!agg.nullable)
    val empty = agg.createAggregationBuffer()
    val nullOnly = agg.createAggregationBuffer()
    agg.update(nullOnly, InternalRow(null))
    assert(empty.toSeq == Seq.fill(10)(0L))
    assert(agg.eval(nullOnly).asInstanceOf[ArrayData].toLongArray().toSeq == empty.toSeq)
    agg.update(nullOnly, InternalRow(0L))
    assert(empty.toSeq == Seq.fill(10)(0L))
    assert(nullOnly(0) == 1L)
  }

  test("all bucket edges survive partial-state serialization and merging") {
    val agg = aggregate
    val values = Seq(
      0L,
      1L,
      9L,
      10L,
      99L,
      100L,
      999L,
      1000L,
      9999L,
      10000L,
      99999L,
      100000L,
      999999L,
      1000000L,
      9999999L,
      10000000L,
      Int.MaxValue.toLong - 1,
      Int.MaxValue.toLong,
      Long.MaxValue)
    val partials = values
      .grouped(9)
      .map { part =>
        val state = agg.createAggregationBuffer()
        part.foreach(value => agg.update(state, InternalRow(value)))
        agg.deserialize(agg.serialize(state))
      }
      .toSeq
    val merged = partials.foldLeft(agg.createAggregationBuffer())(agg.merge)
    val expected = Seq(1L, 2L, 2L, 2L, 2L, 2L, 2L, 2L, 2L, 2L)
    assert(merged.toSeq == expected)
    val bytes = ByteBuffer.allocate(81).order(ByteOrder.nativeOrder())
    bytes.put(10.toByte)
    expected.foreach(bytes.putLong)
    assert(agg.serialize(merged).sameElements(bytes.array()))
  }

  test("row APIs separate mutable and serialized-input slots") {
    val agg = aggregate.withNewMutableAggBufferOffset(2).withNewInputAggBufferOffset(3)
    val writer = aggregate.withNewMutableAggBufferOffset(3)
    val target = new GenericInternalRow(Array[Any](10L, 20L, null, 40L, 50L))
    val source = new GenericInternalRow(Array[Any](60L, 70L, 80L, null, 90L))
    agg.initialize(target)
    writer.initialize(source)
    Seq(0L, 9L).foreach(value => agg.update(target, InternalRow(value)))
    Seq(10L, 100L).foreach(value => writer.update(source, InternalRow(value)))
    writer.serializeAggregateBufferInPlace(source)
    agg.merge(target, source)
    assert(
      agg.eval(target).asInstanceOf[ArrayData].toLongArray().toSeq ==
        Seq(1L, 1L, 1L, 1L, 0L, 0L, 0L, 0L, 0L, 0L))
    assert(Seq(0, 1, 3, 4).forall(index =>
      target.values(index) == Seq[Any](10L, 20L, null, 40L, 50L)(index)))
    assert(Seq(0, 1, 2, 4).forall(index =>
      source.values(index) == Seq[Any](60L, 70L, 80L, null, 90L)(index)))
  }

  test("reference rejects negative values and corrupt bin counts but accepts trailing bytes") {
    val agg = aggregate
    intercept[IllegalArgumentException] {
      agg.update(agg.createAggregationBuffer(), InternalRow(-1L))
    }
    intercept[IllegalArgumentException] {
      agg.merge(Array(1L), Array(1L, 2L))
    }
    val bytes = agg.serialize(agg.createAggregationBuffer())
    val corrupt = bytes.clone()
    corrupt(0) = 9
    intercept[DeltaChecksumException] {
      agg.deserialize(corrupt)
    }
    assert(agg.deserialize(bytes ++ Array(0.toByte)).toSeq == Seq.fill(10)(0L))
  }

  test("update and merge preserve wrapping long counters") {
    val agg = aggregate
    val update = agg.createAggregationBuffer()
    update(0) = Long.MaxValue
    agg.update(update, InternalRow(0L))
    assert(update(0) == Long.MinValue)
    val left = agg.createAggregationBuffer()
    val right = agg.createAggregationBuffer()
    left(0) = Long.MaxValue
    right(0) = 1L
    assert(agg.merge(left, right)(0) == Long.MinValue)
  }
}
