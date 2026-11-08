<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Reference checks for #7573

Run commands from `dev/repros/delta`.

Standalone, test-only probes related to [apache/datafusion-comet#7573](https://github.com/apache/datafusion-comet/issues/7573). They do not implement the requested Comet expression or change admission rules. The issue remains open.

Run with Spark 4.2.0, Scala 2.13.18 and Java 17. Supply matching Delta Spark and storage JARs, version 4.4.0 or 4.4.1. Use a fresh output directory for every invocation.

```bash
mkdir classes
java -Xmx1g -cp "$SPARK_HOME/jars/*" scala.tools.nsc.Main \
  -classpath "$SPARK_HOME/jars/*:$DELTA_SPARK_JAR:$DELTA_STORAGE_JAR" \
  -d classes DeltaInterleaveBitsChecks.scala
jar cf checks.jar -C classes .
"$SPARK_HOME/bin/spark-submit" --master 'local[2]' --driver-memory 2g \
  --jars "$DELTA_SPARK_JAR,$DELTA_STORAGE_JAR" \
  --class org.apache.spark.sql.comet.repro.DeltaInterleaveBitsChecks \
  checks.jar stock /tmp/DeltaInterleaveBitsChecks-stock
"$SPARK_HOME/bin/spark-submit" --master 'local[2]' --driver-memory 2g \
  --jars "$DELTA_SPARK_JAR,$DELTA_STORAGE_JAR,$COMET_JAR" \
  --class org.apache.spark.sql.comet.repro.DeltaInterleaveBitsChecks \
  checks.jar comet /tmp/DeltaInterleaveBitsChecks-comet
```

Expected: 15 named cases, matching row/schema JSON across stock and Comet runs, and `native_acceptance: false` in `summary.json`. The Comet lane requires an accelerated control query; passing Delta probes can still execute in Spark. Execution plans are saved beside rows. These are manually run probes, not tests wired into the Comet CI reactor.

Grounding: selected equivalents of the following versioned upstream tests, not a replacement for their full suites:

- [InterleaveBitsSuite (v4.4.0)](https://github.com/delta-io/delta/blob/v4.4.0/spark/src/test/scala/org/apache/spark/sql/delta/expressions/InterleaveBitsSuite.scala)
- [InterleaveBitsSuite (v4.4.1)](https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/expressions/InterleaveBitsSuite.scala)

Validation uses Comet 1.2.0, not a build of this branch. Current-main expression support and the production image remain unverified.
