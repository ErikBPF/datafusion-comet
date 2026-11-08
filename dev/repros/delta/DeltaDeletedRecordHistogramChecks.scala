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

package org.apache.spark.sql.comet.repro

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.classic.ClassicConversions._

import org.apache.spark.sql.delta.stats.DeletedRecordCountsHistogramUtils.DeletedRecordCountsHistogramAgg
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Selected reference checks for #7571; no expression implementation is added. */
object DeltaDeletedRecordHistogramChecks {
  def main(args: Array[String]): Unit = {
    require(args.length == 2, "usage: stock|comet <fresh output>")
    val mode = args(0); require(Set("stock", "comet")(mode))
    val out = Paths.get(args(1)); Files.createDirectory(out)
    val spark = SparkSession.builder
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension" +
        (if (mode == "comet") ",org.apache.comet.CometSparkSessionExtensions" else ""))
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.comet.enabled", (mode == "comet").toString)
      .config("spark.comet.exec.enabled", (mode == "comet").toString)
      .config("spark.comet.explain.fallback.enabled", "true")
      .config("spark.shuffle.manager", if (mode == "comet")
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"
        else "org.apache.spark.shuffle.sort.SortShuffleManager")
      .config("spark.memory.offHeap.enabled", "true").config("spark.memory.offHeap.size", "512m")
      .config("spark.sql.adaptive.enabled", "false").config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._
    var passed = 0
    def check(name: String)(body: => Unit): Unit = {
      try { body; Files.write(out.resolve(name + ".passed"), Array.emptyByteArray); passed += 1 }
      catch { case e: Throwable => Files.write(out.resolve(name + ".error.txt"), e.toString.getBytes(UTF_8)); throw e }
    }
    def query(name: String, df: DataFrame)(assertRows: Array[Row] => Unit): Unit = check(name) {
      val rows = df.collect(); assertRows(rows)
      Files.write(out.resolve(name + ".rows.json"), rows.map(_.json).sorted.mkString("[", ",", "]").getBytes(UTF_8))
      Files.write(out.resolve(name + ".schema.json"), df.schema.json.getBytes(UTF_8))
      Files.write(out.resolve(name + ".plan.txt"), df.queryExecution.executedPlan.toString.getBytes(UTF_8))
    }
    def rejects[A <: Throwable : scala.reflect.ClassTag](body: => Unit): Unit = {
      var caught = false
      try body catch { case e: Throwable =>
        if (implicitly[scala.reflect.ClassTag[A]].runtimeClass.isInstance(e)) caught = true else throw e }
      require(caught, "expected reference rejection")
    }
    def fallbackObserved(df: DataFrame): Unit = {
      val metrics = df.queryExecution.executedPlan.collect {
        case p if p.nodeName.contains("ObjectHashAggregate") => p.metrics.values.toSeq
      }.flatten
      require(metrics.exists(m => m.name.exists(_.toLowerCase.contains("fall")) && m.value > 0),
        "forced JVM sort fallback must be observed, not inferred from configuration")
    }
    val bound = BoundReference(0, LongType, nullable = true)
    val edges = Seq(0L, 1L, 9L, 10L, 99L, 100L, 999L, 1000L, 9999L, 10000L,
      99999L, 100000L, 999999L, 1000000L, 9999999L, 10000000L,
      Int.MaxValue.toLong - 1, Int.MaxValue.toLong, Long.MaxValue)
    val deletedBins = IndexedSeq(0L,1L,10L,100L,1000L,10000L,100000L,1000000L,10000000L,Int.MaxValue.toLong,Long.MaxValue)
    def deletedCounts(values: Seq[Long]): Seq[Long] = {
      val counts = Array.fill[Long](10)(0)
      values.foreach(v => counts(math.min(9, deletedBins.lastIndexWhere(_ <= v))) += 1)
      counts.toSeq
    }
    def nativeBytes(values: Seq[Long], header: Boolean): Array[Byte] = {
      val b = ByteBuffer.allocate(values.size * 8 + (if (header) 1 else 0)).order(ByteOrder.nativeOrder())
      if (header) b.put(values.size.toByte)
      values.foreach(b.putLong); b.array()
    }
    def deletedCol: Column = Column(DeletedRecordCountsHistogramAgg(UnresolvedAttribute("v")).toAggregateExpression()).as("h")
    try {
      val control = spark.range(20).selectExpr("sum(id) total")
      query("control", control) { rows => require(rows.length == 1 && rows.head.getLong(0) == 190L)
        if (mode == "comet") require(control.queryExecution.executedPlan.toString.contains("Comet")) }
      check("deleted_reference_state") {
        val a = DeletedRecordCountsHistogramAgg(bound); require(!a.nullable && a.dataType == ArrayType(LongType))
        val left = a.createAggregationBuffer(); val right = a.createAggregationBuffer()
        edges.take(9).foreach(v => a.update(left, InternalRow(v))); edges.drop(9).foreach(v => a.update(right, InternalRow(v)))
        a.update(left, InternalRow(null))
        val bytes = a.serialize(left); require(bytes.length == 81 && bytes.sameElements(nativeBytes(left.toSeq, true)))
        require(a.merge(a.deserialize(bytes), right).toSeq == Seq(1L,2L,2L,2L,2L,2L,2L,2L,2L,2L))
        val overflow = a.createAggregationBuffer(); overflow(0) = Long.MaxValue; a.update(overflow, InternalRow(0L)); require(overflow(0) == Long.MinValue)
        val m = a.createAggregationBuffer(); m(0) = Long.MaxValue; val n = a.createAggregationBuffer(); n(0) = 1; require(a.merge(m,n)(0) == Long.MinValue)
        rejects[IllegalArgumentException] { a.update(a.createAggregationBuffer(), InternalRow(-1L)) }
        rejects[IllegalArgumentException] { a.merge(Array(1L), Array(1L,2L)) }
        val corrupt = bytes.clone(); corrupt(0) = 9; rejects[org.apache.spark.sql.delta.DeltaChecksumException] { a.deserialize(corrupt) }
        // The reference accepts some trailing bytes; do not invent stricter compatibility.
        require(a.deserialize(bytes ++ Array(0.toByte)).toSeq == left.toSeq)
      }
      val grouped = (0L until 600L).map(v => (v % 3, v % 120)).toDF("g","v").repartition(2)
      val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq(StructField("g",LongType),StructField("v",LongType))))
      val allNull = spark.range(4).select(lit(0L).as("g"),lit(null).cast("long").as("v"))
      for ((name, agg, oracle) <- Seq[(String, () => Column, Seq[Long] => Seq[Long])](
        ("deleted", () => deletedCol, deletedCounts _))) {
        for ((suffix, input) <- Seq("empty" -> empty, "all_null" -> allNull)) {
          val df = input.agg(agg()); require(!df.schema("h").nullable)
          query(s"${name}_$suffix", df) { rows => require(rows.length == 1 && rows.head.getSeq[Long](0) == oracle(Seq.empty)) }
        }
        query(s"${name}_empty_grouped", empty.groupBy("g").agg(agg()))(rows => require(rows.isEmpty))
        for (threshold <- Seq(128,1)) {
          spark.conf.set("spark.sql.objectHashAggregate.sortBased.fallbackThreshold",threshold)
          val df = grouped.groupBy("g").agg(agg())
          query(s"${name}_grouped_threshold_$threshold", df) { rows =>
            require(rows.length == 3 && rows.map(_.getLong(0)).toSet == Set(0L,1L,2L))
            rows.foreach(r => require(r.getSeq[Long](1) == oracle((0L until 600L).filter(_ % 3 == r.getLong(0)).map(_ % 120))))
            if (threshold == 1) fallbackObserved(df)
          }
        }
      }
      query("deleted_all_bucket_edges", edges.toDF("v").agg(deletedCol)) { rows =>
        require(rows.length == 1 && rows.head.getSeq[Long](0) == Seq(1L,2L,2L,2L,2L,2L,2L,2L,2L,2L)) }
      check("deleted_row_buffer_offsets") {
        // Exercise Spark's row APIs with distinct mutable and serialized-input offsets.
        val a = DeletedRecordCountsHistogramAgg(bound)
          .withNewMutableAggBufferOffset(2).withNewInputAggBufferOffset(3)
        val writer = DeletedRecordCountsHistogramAgg(bound).withNewMutableAggBufferOffset(3)
        val target = new GenericInternalRow(Array[Any](10L, 20L, null, 40L, 50L))
        val source = new GenericInternalRow(Array[Any](60L, 70L, 80L, null, 90L))
        a.initialize(target)
        writer.initialize(source)
        Seq(0L, 9L).foreach(v => a.update(target, InternalRow(v)))
        Seq(10L, 100L).foreach(v => writer.update(source, InternalRow(v)))
        writer.serializeAggregateBufferInPlace(source)
        a.merge(target, source)
        require(a.eval(target).asInstanceOf[ArrayData].toLongArray().toSeq == deletedCounts(Seq(0L, 9L, 10L, 100L)))
        require(Seq(0, 1, 3, 4).forall(i => target.values(i) == Seq[Any](10L, 20L, null, 40L, 50L)(i)),
          "mutable aggregate buffer overwrote another aggregate's slot")
        require(Seq(0, 1, 2, 4).forall(i => source.values(i) == Seq[Any](60L, 70L, 80L, null, 90L)(i)),
          "serialization overwrote another aggregate's input slot")
      }
      spark.conf.set("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", 1)
      val otherHistogram = Column(DeletedRecordCountsHistogramAgg(UnresolvedAttribute("w")).toAggregateExpression()).as("other")
      val mixed = grouped.withColumn("w", col("v") + 100L).groupBy("g")
        .agg(count(col("v")).as("n"), deletedCol, sum(col("w")).as("total"), otherHistogram)
      require(Seq("h", "other").forall(name =>
        mixed.schema(name).dataType == ArrayType(LongType) && !mixed.schema(name).nullable))
      query("deleted_mixed_aggregate_slots", mixed) { rows =>
        require(rows.length == 3 && rows.map(_.getLong(0)).toSet == Set(0L, 1L, 2L))
        rows.foreach { row =>
          val values = (0L until 600L).filter(_ % 3 == row.getLong(0)).map(_ % 120)
          require(row.getLong(1) == values.size)
          require(row.getSeq[Long](2) == deletedCounts(values))
          require(row.getLong(3) == values.map(_ + 100L).sum)
          require(row.getSeq[Long](4) == deletedCounts(values.map(_ + 100L)))
        }
        fallbackObserved(mixed)
      }
      require(passed == 10, s"expected 10 named cases, observed $passed")
      Files.write(out.resolve("summary.json"),s"{\"mode\":\"$mode\",\"passed\":$passed,\"native_acceptance\":false}".getBytes(UTF_8))
      println(s"PASS DeltaDeletedRecordHistogramChecks $mode cases=$passed (reference/fallback only)")
    } finally spark.stop()
  }
}
