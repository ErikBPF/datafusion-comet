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

# Reference checks for #7455

Run commands from `dev/repros/delta`.

Standalone, test-only probes related to [apache/datafusion-comet#7455](https://github.com/apache/datafusion-comet/issues/7455). They do not implement Spark/Delta pairing support or change admission rules. The issue remains open.

This Java helper checks Hadoop 3.5.0 encryption-context resolution, synthetic local JCEKS credential providers, and bucket-option propagation. It never creates an S3 client, calls AWS, or exercises Comet admission. It does not repair the failing sensitive-key classification assertion. Coordinate that repair with existing issue #7455 and upstream PR #7026.

Use Java 17. `HADOOP_COMPILE_CP` must contain Hadoop client API 3.5.0 and Hadoop AWS 3.5.0. `HADOOP_RUNTIME_CP` must include their runtime dependencies (the classpath captured by the DeltaScanContribSuite test run is suitable). Use synthetic, disposable paths only; unset `HADOOP_CREDSTORE_PASSWORD` before running.

```bash
mkdir classes
javac -J-Xmx256m -proc:none -cp "$HADOOP_COMPILE_CP" \
  -d classes S3ContextIntegrationEquivalents.java
java -Xmx256m -cp "classes:$HADOOP_RUNTIME_CP" \
  S3ContextIntegrationEquivalents /tmp/s3-context-checks
```

Expected: 38 explicit cases. The helper refuses an inherited credential-store password and uses temporary synthetic stores. Run on disposable local storage; no real credentials are required.

Grounding: Hadoop 3.5.0 `S3AEncryption.getS3EncryptionContext`, credential-provider lookup, `propagateBucketOptions`, and Comet `DeltaScanContribSuite` are distinct boundaries. No direct Spark encryption-context test was found in the bounded source search; this is a helper equivalent, not Spark or AWS certification.

- [Hadoop S3AEncryption (3.5.0)](https://github.com/apache/hadoop/blob/rel/release-3.5.0/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/impl/S3AEncryption.java)
- [Comet DeltaScanContribSuite (review base)](https://github.com/apache/datafusion-comet/blob/4de6704391c9a7832a2079fc4e8d80fc09e9b209/contrib/delta-spark/src/test/scala/org/apache/comet/contrib/delta/DeltaScanContribSuite.scala)
