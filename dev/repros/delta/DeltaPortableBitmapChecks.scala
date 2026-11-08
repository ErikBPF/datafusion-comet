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


import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{BoundReference}
import org.apache.spark.sql.catalyst.expressions.aggregation.BitmapAggregator
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Selected reference checks for #7572; no expression implementation is added. */
object DeltaPortableBitmapChecks {
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
    val bitmapType = StructType(Seq(StructField("cardinality", LongType), StructField("last", LongType), StructField("bitmap", BinaryType)))
    def bitmapCol: Column = Column(new BitmapAggregator(UnresolvedAttribute("v"), RoaringBitmapArrayFormat.Portable).toAggregateExpression()).as("bitmap")
    def bitmapRow(row: Row, expected: Seq[Long]): Unit = {
      val r = row.getStruct(row.length - 1); require(r.getLong(0) == expected.size)
      if (expected.isEmpty) require(r.isNullAt(1)) else require(r.getLong(1) == expected.last)
      val bytes: Array[Byte] = r.getAs(2)
      require(RoaringBitmapArray.readFrom(bytes).values.sorted.toSeq == expected)
    }
    def bitmapSchema(df: DataFrame): Unit = {
      val f = df.schema("bitmap"); require(f.dataType == bitmapType && !f.nullable)
    }
    try {
      val control = spark.range(20).selectExpr("sum(id) total")
      query("control", control) { rows => require(rows.length == 1 && rows.head.getLong(0) == 190L)
        if (mode == "comet") require(control.queryExecution.executedPlan.toString.contains("Comet")) }
      val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq(StructField("g",LongType),StructField("v",LongType))))
      val allNull = spark.range(4).select(lit(0L).as("g"),lit(null).cast("long").as("v"))
      val special = Seq(0L,1L,65535L,65536L,2147483647L,2147483648L,4294967295L,4294967296L,4294967299L)
      check("bitmap_reference_state") {
        val a = new BitmapAggregator(bound, RoaringBitmapArrayFormat.Portable)
        require(a.dataType == bitmapType && !a.nullable)
        val expected = special.sorted
        def state(xs: Seq[Long]): RoaringBitmapArray = { val b = a.createAggregationBuffer(); xs.foreach(v => a.update(b,InternalRow(v))); a.update(b,InternalRow(null)); b }
        val left = a.deserialize(a.serialize(state(special.take(5)))); val right = a.deserialize(a.serialize(state(special.drop(5))))
        val merged = a.merge(left,right); require(merged.values.sorted.toSeq == expected)
        require(right.values.sorted.toSeq == special.drop(5).sorted)
        require(a.merge(merged,a.deserialize(a.serialize(right))).values.sorted.toSeq == expected)
        val reverse = a.merge(state(special.drop(5)),state(special.take(5))); require(reverse.values.sorted.toSeq == expected)
        val emptyState = a.eval(a.createAggregationBuffer()); require(emptyState.getLong(0) == 0 && emptyState.isNullAt(1))
        for (bad <- Seq(-1L,Long.MaxValue)) rejects[IllegalArgumentException] { a.update(a.createAggregationBuffer(),InternalRow(bad)) }
      }
      for ((suffix,input) <- Seq("empty" -> empty,"all_null" -> allNull)) {
        val df = input.agg(bitmapCol); bitmapSchema(df)
        query(s"bitmap_$suffix",df)(rows => {require(rows.length == 1);bitmapRow(rows.head,Seq.empty)})
      }
      query("bitmap_empty_grouped",empty.groupBy("g").agg(bitmapCol))(rows => require(rows.isEmpty))
      val bitmapInput = special.zipWithIndex.flatMap { case (v,i) => Seq((i % 2,Option(v)),(i % 2,Option(v)),(i % 2,None)) }.toDF("g","v")
      for (partitions <- Seq(2,3)) {
        val input = if (partitions == 2) bitmapInput.repartition(2,col("g")) else bitmapInput.repartition(3)
        val df = input.groupBy("g").agg(bitmapCol); bitmapSchema(df)
        query(s"bitmap_grouped_$partitions",df)(rows => {require(rows.length == 2 && rows.map(_.getInt(0)).toSet == Set(0,1));rows.foreach(r => bitmapRow(r,special.zipWithIndex.filter(_._2 % 2 == r.getInt(0)).map(_._1).distinct.sorted))})
      }
      val dense = spark.range(65537).select(col("id").as("v")).agg(bitmapCol); bitmapSchema(dense)
      query("bitmap_dense_container_boundary",dense)(rows => {require(rows.length == 1);bitmapRow(rows.head,0L to 65536L)})
      spark.conf.set("spark.sql.objectHashAggregate.sortBased.fallbackThreshold",1)
      val bitmapFallback = bitmapInput.repartition(2).groupBy("g").agg(bitmapCol)
      query("bitmap_forced_jvm_fallback",bitmapFallback)(rows => {require(rows.length == 2 && rows.map(_.getInt(0)).toSet == Set(0,1));rows.foreach(r => bitmapRow(r,special.zipWithIndex.filter(_._2 % 2 == r.getInt(0)).map(_._1).distinct.sorted));fallbackObserved(bitmapFallback)})
      spark.conf.set("spark.sql.objectHashAggregate.sortBased.fallbackThreshold",128)
      require(passed == 9, s"expected 9 named cases, observed $passed")
      Files.write(out.resolve("summary.json"),s"{\"mode\":\"$mode\",\"passed\":$passed,\"native_acceptance\":false}".getBytes(UTF_8))
      println(s"PASS DeltaPortableBitmapChecks $mode cases=$passed (reference/fallback only)")
    } finally spark.stop()
  }
}
