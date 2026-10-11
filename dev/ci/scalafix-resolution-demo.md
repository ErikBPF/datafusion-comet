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

# Scalafix plugin-resolution demonstration

Reproducer for the retry discussion in [PR #6061](https://github.com/apache/datafusion-comet/pull/6061), based on its head `72244ad5c`.

From the repository root, with JDK 17 and Python 3 available:

```sh
python3 dev/ci/reproduce-scalafix-resolution.py
```

The Maven wrapper may download Maven on its first invocation. The reproduction then uses a loopback HTTP repository that returns 503 for Scalafix plugin artifacts, with a fresh isolated Maven repository for every case. Temporary files are removed afterward. No native build or Spark installation is needed.

Four cases exercise real Maven resolution for Scala 2.12 and 2.13:

| Invocation                                               | Expected error                               |
| -------------------------------------------------------- | -------------------------------------------- |
| `scalafix:scalafix`                                      | Prefix lookup fails, hiding the HTTP 503     |
| `io.github.evis:scalafix-maven-plugin_<binary>:scalafix` | Transfer failure includes the HTTP 503 cause |

Both forms request the version declared in the root POM's plugin management. The script verifies the requested artifact path and prints the error from each invocation. Maven is expected to fail in all four cases; the demonstration script succeeds only when the expected distinction is observed.

This demonstrates why a retry classifier needs the qualified goal to recognize this outage reliably. It does not simulate lint violations or prove recovery after an outage ends. The proposed workflow changes remain a separate follow-up for discussion.
