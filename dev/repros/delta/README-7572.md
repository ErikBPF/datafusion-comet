# Reference checks for #7572

Run commands from `dev/repros/delta`.

Standalone, test-only probes related to [apache/datafusion-comet#7572](https://github.com/apache/datafusion-comet/issues/7572). They do not implement the requested Comet expression or change admission rules. The issue remains open.

Run with Spark 4.2.0, Scala 2.13.18 and Java 17. Supply matching Delta Spark and storage JARs, version 4.4.0 or 4.4.1. Use a fresh output directory for every invocation.

```bash
mkdir classes
java -Xmx1g -cp "$SPARK_HOME/jars/*" scala.tools.nsc.Main \
  -classpath "$SPARK_HOME/jars/*:$DELTA_SPARK_JAR:$DELTA_STORAGE_JAR" \
  -d classes DeltaPortableBitmapChecks.scala
jar cf checks.jar -C classes .
"$SPARK_HOME/bin/spark-submit" --master 'local[2]' --driver-memory 2g \
  --jars "$DELTA_SPARK_JAR,$DELTA_STORAGE_JAR" \
  --class org.apache.spark.sql.comet.repro.DeltaPortableBitmapChecks \
  checks.jar stock /tmp/DeltaPortableBitmapChecks-stock
"$SPARK_HOME/bin/spark-submit" --master 'local[2]' --driver-memory 2g \
  --jars "$DELTA_SPARK_JAR,$DELTA_STORAGE_JAR,$COMET_JAR" \
  --class org.apache.spark.sql.comet.repro.DeltaPortableBitmapChecks \
  checks.jar comet /tmp/DeltaPortableBitmapChecks-comet
```

Expected: 9 named cases, matching row/schema JSON across stock and Comet runs, and `native_acceptance: false` in `summary.json`. The Comet lane requires an accelerated control query; passing Delta probes can still execute in Spark. Execution plans are saved beside rows. These are manually run probes, not tests wired into the Comet CI reactor.

Grounding: selected equivalents of the following versioned upstream tests, not a replacement for their full suites:

- [TypedImperativeAggregateSuite (v4.2.0)](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/test/scala/org/apache/spark/sql/TypedImperativeAggregateSuite.scala)
- [ObjectHashAggregateSuite (v4.2.0)](https://github.com/apache/spark/blob/v4.2.0/sql/hive/src/test/scala/org/apache/spark/sql/hive/execution/ObjectHashAggregateSuite.scala)
- [BitmapAggregatorE2ESuite (v4.4.0)](https://github.com/delta-io/delta/blob/v4.4.0/spark/src/test/scala/org/apache/spark/sql/delta/util/BitmapAggregatorE2ESuite.scala)
- [RoaringBitmapArraySuite (v4.4.0)](https://github.com/delta-io/delta/blob/v4.4.0/spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/RoaringBitmapArraySuite.scala)
- [BitmapAggregatorE2ESuite (v4.4.1)](https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/util/BitmapAggregatorE2ESuite.scala)
- [RoaringBitmapArraySuite (v4.4.1)](https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/RoaringBitmapArraySuite.scala)

Validation uses Comet 1.2.0, not a build of this branch. Current-main expression support and the production image remain unverified.
