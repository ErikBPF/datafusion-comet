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
import org.apache.spark.sql.catalyst.expressions.{BoundReference}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.classic.ClassicConversions._

import org.apache.spark.sql.delta.stats.FileSizeHistogramUtils.FileSizeHistogramAgg
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Selected reference checks for #7570; no expression implementation is added. */
object DeltaFileSizeHistogramChecks {
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
    def fallbackObserved(df: DataFrame): Unit = {
      val metrics = df.queryExecution.executedPlan.collect {
        case p if p.nodeName.contains("ObjectHashAggregate") => p.metrics.values.toSeq
      }.flatten
      require(metrics.exists(m => m.name.exists(_.toLowerCase.contains("fall")) && m.value > 0),
        "forced JVM sort fallback must be observed, not inferred from configuration")
    }
    val bound = BoundReference(0, LongType, nullable = true)
    val bins = IndexedSeq(-10L, 0L, 10L, 100L)
    def sizes(values: Seq[Long]): Seq[Long] = {
      val counts = Array.fill[Long](bins.size)(0); val totals = counts.clone()
      values.foreach { v => val i = bins.lastIndexWhere(_ <= v); if (i >= 0) { counts(i) += 1; totals(i) += v } }
      bins ++ counts ++ totals
    }
    def nativeBytes(values: Seq[Long], header: Boolean): Array[Byte] = {
      val b = ByteBuffer.allocate(values.size * 8 + (if (header) 1 else 0)).order(ByteOrder.nativeOrder())
      if (header) b.put(values.size.toByte)
      values.foreach(b.putLong); b.array()
    }
    def sizeCol: Column = Column(FileSizeHistogramAgg(UnresolvedAttribute("v"), bins).toAggregateExpression()).as("h")
    try {
      val control = spark.range(20).selectExpr("sum(id) total")
      query("control", control) { rows => require(rows.length == 1 && rows.head.getLong(0) == 190L)
        if (mode == "comet") require(control.queryExecution.executedPlan.toString.contains("Comet")) }
      check("filesize_reference_state") {
        val a = FileSizeHistogramAgg(bound, bins)
        require(a.dataType == ArrayType(LongType) && !a.nullable)
        val left = a.createAggregationBuffer(); val right = a.createAggregationBuffer()
        Seq[java.lang.Long](-11L,-10L,-5L,0L,null).foreach(v => a.update(left, InternalRow(v)))
        Seq(9L,10L,99L,100L).foreach(v => a.update(right, InternalRow(v)))
        require(a.serialize(left).sameElements(nativeBytes(left.toSeq, false)))
        val merged = a.merge(a.deserialize(a.serialize(left)), a.deserialize(a.serialize(right)))
        require(a.eval(merged).asInstanceOf[ArrayData].toLongArray().toSeq == sizes(Seq(-11L,-10L,-5L,0L,9L,10L,99L,100L)))
        val independent = a.createAggregationBuffer(); a.update(independent, InternalRow(1L))
        require(!independent.eq(left) && left(1) == 1L)
        val counter = a.createAggregationBuffer(); counter(1) = Long.MaxValue; a.update(counter, InternalRow(0L)); require(counter(1) == Long.MinValue)
        val total = a.createAggregationBuffer(); Seq(Long.MaxValue,Long.MaxValue).foreach(v => a.update(total, InternalRow(v))); require(total.last == -2L)
        require(a.withNewMutableAggBufferOffset(2).mutableAggBufferOffset == 2)
        require(a.withNewInputAggBufferOffset(3).inputAggBufferOffset == 3)
      }
      val grouped = (0L until 600L).map(v => (v % 3, v % 120)).toDF("g","v").repartition(2)
      val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq(StructField("g",LongType),StructField("v",LongType))))
      val allNull = spark.range(4).select(lit(0L).as("g"),lit(null).cast("long").as("v"))
      for ((name, agg, oracle) <- Seq[(String, () => Column, Seq[Long] => Seq[Long])](
        ("filesize", () => sizeCol, sizes _))) {
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
      require(passed == 7, s"expected 7 named cases, observed $passed")
      Files.write(out.resolve("summary.json"),s"{\"mode\":\"$mode\",\"passed\":$passed,\"native_acceptance\":false}".getBytes(UTF_8))
      println(s"PASS DeltaFileSizeHistogramChecks $mode cases=$passed (reference/fallback only)")
    } finally spark.stop()
  }
}
