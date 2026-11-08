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

# Reference checks for #7571

Run commands from `dev/repros/delta`.

Standalone, test-only probes related to [apache/datafusion-comet#7571](https://github.com/apache/datafusion-comet/issues/7571). They do not implement the requested Comet expression or change admission rules. The issue remains open.

Run with Spark 4.2.0, Scala 2.13.18 and Java 17. Supply matching Delta Spark and storage JARs, version 4.4.0 or 4.4.1. Use a fresh output directory for every invocation.

The setup requires `curl`. Set `SPARK_HOME` and `JAVA_HOME` to your local installations before running it. The Comet artifact is `org.apache.datafusion:comet-spark-spark4.2_2.13:1.2.0`; match the Spark 4.2 and Scala 2.13 lines. This assembly contains the platform library used by the validated Linux x86_64 run; no separate Comet companion JAR was used. Other platforms remain unverified.

```bash
set -eu
: "${SPARK_HOME:?Set SPARK_HOME to a Spark 4.2.0 installation}"
: "${JAVA_HOME:?Set JAVA_HOME to a Java 17 installation}"
export PATH="$JAVA_HOME/bin:$PATH"
DELTA_VERSION=4.4.0 # Use 4.4.1 to repeat the other validated pairing.
ARTIFACT_DIR=$(mktemp -d)
export DELTA_SPARK_JAR="$ARTIFACT_DIR/delta-spark_2.13-$DELTA_VERSION.jar"
export DELTA_STORAGE_JAR="$ARTIFACT_DIR/delta-storage-$DELTA_VERSION.jar"
export COMET_JAR="$ARTIFACT_DIR/comet-spark-spark4.2_2.13-1.2.0.jar"
curl --fail --location --retry 3 -o "$DELTA_SPARK_JAR" \
  "https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.13/$DELTA_VERSION/delta-spark_2.13-$DELTA_VERSION.jar"
curl --fail --location --retry 3 -o "$DELTA_STORAGE_JAR" \
  "https://repo.maven.apache.org/maven2/io/delta/delta-storage/$DELTA_VERSION/delta-storage-$DELTA_VERSION.jar"
curl --fail --location --retry 3 -o "$COMET_JAR" \
  https://repo.maven.apache.org/maven2/org/apache/datafusion/comet-spark-spark4.2_2.13/1.2.0/comet-spark-spark4.2_2.13-1.2.0.jar
mkdir classes
java -Xmx1g -cp "$SPARK_HOME/jars/*" scala.tools.nsc.Main \
  -classpath "$SPARK_HOME/jars/*:$DELTA_SPARK_JAR:$DELTA_STORAGE_JAR" \
  -d classes DeltaDeletedRecordHistogramChecks.scala
jar cf checks.jar -C classes .
"$SPARK_HOME/bin/spark-submit" --master 'local[2]' --driver-memory 2g \
  --jars "$DELTA_SPARK_JAR,$DELTA_STORAGE_JAR" \
  --class org.apache.spark.sql.comet.repro.DeltaDeletedRecordHistogramChecks \
  checks.jar stock /tmp/DeltaDeletedRecordHistogramChecks-stock
"$SPARK_HOME/bin/spark-submit" --master 'local[2]' --driver-memory 2g \
  --jars "$DELTA_SPARK_JAR,$DELTA_STORAGE_JAR,$COMET_JAR" \
  --class org.apache.spark.sql.comet.repro.DeltaDeletedRecordHistogramChecks \
  checks.jar comet /tmp/DeltaDeletedRecordHistogramChecks-comet
```

Expected: 10 named cases, matching row/schema JSON across stock and Comet runs, and `native_acceptance: false` in `summary.json`. The Comet lane requires an accelerated control query; passing Delta probes can still execute in Spark. Execution plans are saved beside rows. These are manually run probes, not tests wired into the Comet CI reactor.

Two checks pin mixed aggregate state: the row API uses mutable slot 2 and serialized input slot 3 while preserving neighboring slots, and a grouped query mixes `COUNT`, two histograms over different inputs, and `SUM` under observed JVM sort fallback. These checks exercise Spark aggregate buffer routing; they do not test a Comet-produced partial state.

Grounding: selected equivalents of the following versioned upstream tests, not a replacement for their full suites:

- [TypedImperativeAggregateSuite (v4.2.0)](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/test/scala/org/apache/spark/sql/TypedImperativeAggregateSuite.scala)
- [ObjectHashAggregateSuite (v4.2.0)](https://github.com/apache/spark/blob/v4.2.0/sql/hive/src/test/scala/org/apache/spark/sql/hive/execution/ObjectHashAggregateSuite.scala)
- [ChecksumDVMetricsSuite (v4.4.0)](https://github.com/delta-io/delta/blob/v4.4.0/spark/src/test/scala/org/apache/spark/sql/delta/ChecksumDVMetricsSuite.scala)
- [ChecksumDVMetricsSuite (v4.4.1)](https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/ChecksumDVMetricsSuite.scala)

Validation uses Comet 1.2.0, not a build of this branch. Current-main expression support and the production image remain unverified.

## Automated reference-state coverage

[DeletedRecordHistogramSuite](../../../contrib/delta-spark/src/test/scala/org/apache/spark/sql/delta/DeletedRecordHistogramSuite.scala) binds five row-state contract tests into the existing Delta contrib ScalaTest module. They cover null/empty state, all bucket edges and exact serialization, real mutable/input buffer slots, rejection semantics, and wrapping counters. The suite uses Delta's package because Delta 3.3 and 4.0 keep this helper `private[delta]`.

The existing Delta contrib workflow discovers this suite with the rest of the module. It runs on the existing Spark 3.5 / Delta 3.3.2, Spark 4.0 / Delta 4.0.1, and Spark 4.1 / Delta 4.3.1 matrix in nightly and `run-delta-tests` jobs. No workflow or version-matrix change is required. Contrib suites are intentionally excluded from the main Linux/macOS suite registry.

After building Comet as described in the contributor guide, run from the repository root:

```bash
set -eu
report=contrib/delta-spark/target/surefire-reports/TEST-org.apache.spark.sql.delta.DeletedRecordHistogramSuite.xml
rm -f "$report"
./mvnw -B -Pspark-3.5,delta test -Dtest=none \
  -DwildcardSuites=org.apache.spark.sql.delta
python3 - "$report" <<'PYTHON'
import sys
import xml.etree.ElementTree as ET
suite = ET.parse(sys.argv[1]).getroot()
assert suite.get("name") == "org.apache.spark.sql.delta.DeletedRecordHistogramSuite"
cases = suite.findall("testcase")
assert len(cases) == 5
assert all(case.find(tag) is None for case in cases
           for tag in ("failure", "error", "skipped"))
print("PASS DeletedRecordHistogramSuite: 5 tests, no failures or skips")
PYTHON
```

Package discovery avoids trying to load the contrib-only suite in preceding reactor modules. The report is removed before execution so a stale report cannot satisfy the five-case gate. Zero discovery in preceding modules is not test evidence.

Repeat with `-Pspark-4.0,delta` and `-Pspark-4.1,delta` for the other existing pairs. Require five passing tests with zero canceled/ignored tests per profile. These are Spark/Delta reference tests without a Spark session or Comet JNI. They do not replace the grouped Spark/Comet flow probes above or certify Rust/JVM state interoperability. Spark 4.2 / Delta 4.4 remains the separately validated manual pairing, not a new CI support claim.
