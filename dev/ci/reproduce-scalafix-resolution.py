#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Reproduce Maven's hidden Scalafix transfer error against a local HTTP 503 server."""

import http.server
from pathlib import Path
import subprocess
import tempfile
import threading
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
VERSION = ET.parse(ROOT / "pom.xml").findtext(
    "m:properties/m:scalafix-maven-plugin.version",
    namespaces={"m": "http://maven.apache.org/POM/4.0.0"})


class Repository(http.server.BaseHTTPRequestHandler):
    requests = []

    def do_GET(self):
        self.requests.append(self.path)
        if self.path.endswith("maven-metadata.xml"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"<metadata><plugins/></metadata>")
        else:
            self.send_response(503 if "scalafix-maven-plugin" in self.path else 404)
            self.end_headers()

    def log_message(self, *args):
        pass


def main():
    assert VERSION, "Expected the Scalafix plugin version in the root POM"
    subprocess.run([str(ROOT / "mvnw"), "--version"], cwd=ROOT, check=True, timeout=120)
    with tempfile.TemporaryDirectory(prefix="scalafix-resolution-") as directory:
        with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Repository) as server:
            threading.Thread(target=server.serve_forever, daemon=True).start()
            try:
                for binary in ("2.13", "2.12"):
                    for mode in ("prefix", "qualified-managed"):
                        work = Path(directory) / f"{binary}-{mode}"
                        work.mkdir()
                        artifact = f"scalafix-maven-plugin_{binary}"
                        (work / "pom.xml").write_text(f"""<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>demo</groupId><artifactId>demo</artifactId><version>1</version>
  <profiles><profile><id>semanticdb</id><build><pluginManagement><plugins><plugin>
    <groupId>io.github.evis</groupId><artifactId>{artifact}</artifactId>
    <version>{VERSION}</version>
  </plugin></plugins></pluginManagement></build></profile></profiles>
</project>
""")
                        (work / "settings.xml").write_text(f"""<settings>
  <mirrors><mirror><id>controlled</id><mirrorOf>*</mirrorOf>
    <url>http://127.0.0.1:{server.server_port}/</url>
  </mirror></mirrors>
</settings>
""")
                        goal = ("scalafix:scalafix" if mode == "prefix" else
                                f"io.github.evis:{artifact}:scalafix")
                        Repository.requests.clear()
                        result = subprocess.run(
                            [str(ROOT / "mvnw"), "-B", "-f", str(work / "pom.xml"),
                             "-s", str(work / "settings.xml"),
                             f"-Dmaven.repo.local={work / 'repository'}",
                             "-Daether.connector.http.retryHandler.count=0",
                             "-Psemanticdb", goal],
                            cwd=ROOT, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=60)
                        requested_pom = (f"/io/github/evis/{artifact}/{VERSION}/"
                                         f"{artifact}-{VERSION}.pom")
                        errors = [line for line in result.stdout.splitlines()
                                  if line.startswith("[ERROR]")]
                        exposed = any("Could not transfer artifact" in line and
                                      "status code: 503" in line for line in errors)
                        assert result.returncode != 0, result.stdout
                        assert requested_pom in Repository.requests, result.stdout
                        assert exposed == (mode == "qualified-managed"), result.stdout
                        if mode == "prefix":
                            assert "No plugin found for prefix 'scalafix'" in result.stdout
                        print(f"Scala {binary}, {mode}: managed version {VERSION}, "
                              f"HTTP 503 exposed in error: {exposed}", flush=True)
                        print(errors[0], flush=True)
            finally:
                server.shutdown()


if __name__ == "__main__":
    main()
