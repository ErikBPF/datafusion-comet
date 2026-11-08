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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.impl.S3AEncryption;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

/** Hadoop 3.5 helper equivalents only: no Spark/Comet admission or S3 reader is exercised. */
public final class S3ContextIntegrationEquivalents {
  private static final String BUCKET = "synthetic-bucket";
  private static final String KEY = "fs.s3a.encryption.context";
  private static final String SHORT = "fs.s3a.bucket." + BUCKET + ".encryption.context";
  private static final String LONG = "fs.s3a.bucket." + BUCKET + "." + KEY;
  private static final String PROVIDERS = "hadoop.security.credential.provider.path";
  private static final String FALLBACK = "hadoop.security.credential.clear-text-fallback";
  private static final String VALID = "purpose=synthetic";
  private static final String MALFORMED = "synthetic-malformed-pair";
  private static final List<String> EXPECTED = List.of(
      "absent_raw", "absent_propagated",
      "global_valid_raw", "global_valid_propagated", "global_malformed_raw", "global_malformed_propagated",
      "short_valid_raw", "short_valid_propagated", "short_malformed_raw", "short_malformed_propagated",
      "long_valid_raw", "long_valid_propagated", "long_malformed_raw", "long_malformed_propagated",
      "provider_valid_shadow_true_raw", "provider_valid_shadow_true_propagated",
      "provider_valid_shadow_false_raw", "provider_valid_shadow_false_propagated",
      "provider_malformed_shadow_true_raw", "provider_malformed_shadow_true_propagated",
      "provider_malformed_shadow_false_raw", "provider_malformed_shadow_false_propagated",
      "provider_missing_valid_true_raw", "provider_missing_valid_true_propagated",
      "provider_missing_valid_false_raw", "provider_missing_valid_false_propagated",
      "provider_missing_malformed_true_raw", "provider_missing_malformed_true_propagated",
      "provider_missing_malformed_false_raw", "provider_missing_malformed_false_propagated",
      "provider_short_valid_raw", "provider_short_valid_propagated",
      "provider_long_valid_raw", "provider_long_valid_propagated",
      "provider_short_malformed_raw", "provider_short_malformed_propagated",
      "provider_long_malformed_raw", "provider_long_malformed_propagated");
  private static final List<String> passed = new ArrayList<>();
  private static String active = "setup";

  private static Configuration fresh() {
    Configuration conf = new Configuration(false);
    if (conf.get(PROVIDERS) != null || conf.get("fs.s3a.security.credential.provider.path") != null) {
      throw new AssertionError("unexpected provider configuration");
    }
    return conf;
  }

  private static void check(String name, Configuration conf, String expected, boolean malformed)
      throws Exception {
    active = name;
    if (!EXPECTED.contains(name) || passed.contains(name)) throw new AssertionError("case selection");
    if (malformed) {
      if (!MALFORMED.equals(S3AEncryption.getS3EncryptionContext(BUCKET, conf))) {
        throw new AssertionError("malformed raw context mismatch");
      }
      rejects(() -> S3AEncryption.getS3EncryptionContextBase64Encoded(BUCKET, conf, false));
      rejects(() -> S3AUtils.buildEncryptionSecrets(BUCKET, conf));
    } else {
      String encoded = expected.isEmpty() ? "" : Base64.getEncoder().encodeToString(
          "{\"purpose\":\"synthetic\"}".getBytes(StandardCharsets.UTF_8));
      if (!expected.equals(S3AEncryption.getS3EncryptionContext(BUCKET, conf))) {
        throw new AssertionError("raw context mismatch");
      }
      if (!encoded.equals(S3AEncryption.getS3EncryptionContextBase64Encoded(BUCKET, conf, false))) {
        throw new AssertionError("encoded context mismatch");
      }
      if (!encoded.equals(S3AUtils.buildEncryptionSecrets(BUCKET, conf).getEncryptionContext())) {
        throw new AssertionError("secrets context mismatch");
      }
    }
    passed.add(name);
    System.out.println("PASS " + name + (malformed ? " expected=java.lang.IllegalArgumentException" : ""));
  }

  private interface Call { Object run() throws Exception; }
  private static void rejects(Call call) throws Exception {
    try { call.run(); }
    catch (IllegalArgumentException expected) { return; }
    throw new AssertionError("malformed context accepted");
  }

  private static void pair(String name, Configuration conf, String expected, boolean malformed)
      throws Exception {
    check(name + "_raw", conf, expected, malformed);
    check(name + "_propagated", S3AUtils.propagateBucketOptions(conf, BUCKET), expected, malformed);
  }

  private static Configuration store(Path out, String name, String alias, String value,
      boolean fallback) throws Exception {
    Configuration conf = fresh();
    conf.setBoolean(FALLBACK, fallback);
    Path path = out.resolve(name + ".jceks");
    if (Files.exists(path)) throw new AssertionError("store already exists");
    conf.set(PROVIDERS, "jceks://file" + path.toAbsolutePath());
    CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);
    if (alias != null) provider.createCredentialEntry(alias, value.toCharArray());
    provider.flush();
    return conf;
  }

  public static void main(String[] args) {
    Path out = null;
    List<Path> stores = new ArrayList<>();
    boolean created = false;
    int status = 1;
    String failureClass = "";
    try {
      if (args.length != 1) throw new IllegalArgumentException("one fresh output directory required");
      // Refuse credential-store password inheritance; never print its value.
      if (System.getenv("HADOOP_CREDSTORE_PASSWORD") != null) {
        throw new IllegalStateException("credential-store password environment must be unset");
      }
      out = Path.of(args[0]).toAbsolutePath();
      Files.createDirectory(out);
      created = true;
      System.out.println("S3_CONTEXT_EQUIVALENTS_EXPECTED cases=" + EXPECTED.size());
      pair("absent", fresh(), "", false);
      String[] forms = {"global", "short", "long"};
      String[] keys = {KEY, SHORT, LONG};
      for (int i = 0; i < keys.length; i++) {
        for (boolean malformed : new boolean[] {false, true}) {
          Configuration conf = fresh();
          conf.set("fs.s3a.encryption.algorithm", "SSE-KMS");
          conf.set(keys[i], malformed ? MALFORMED : VALID);
          pair(forms[i] + (malformed ? "_malformed" : "_valid"), conf, VALID, malformed);
        }
      }
      for (boolean fallback : new boolean[] {true, false}) {
        for (boolean malformed : new boolean[] {false, true}) {
          String name = "provider_" + (malformed ? "malformed" : "valid") + "_shadow_" + fallback;
          stores.add(out.resolve(name + ".jceks"));
          Configuration conf = store(out, name, KEY, malformed ? MALFORMED : VALID, fallback);
          conf.set(KEY, malformed ? VALID : MALFORMED);
          pair(name, conf, VALID, malformed);
        }
        for (boolean malformed : new boolean[] {false, true}) {
          String name = "provider_missing_" + (malformed ? "malformed" : "valid") + "_" + fallback;
          stores.add(out.resolve(name + ".jceks"));
          Configuration conf = store(out, name, "synthetic.unrelated.alias", VALID, fallback);
          conf.set(KEY, malformed ? MALFORMED : VALID);
          pair(name, conf, fallback ? VALID : "", fallback && malformed);
        }
      }
      for (int i = 1; i < keys.length; i++) {
        for (boolean malformed : new boolean[] {false, true}) {
          String name = "provider_" + forms[i] + (malformed ? "_malformed" : "_valid");
          stores.add(out.resolve(name + ".jceks"));
          Configuration conf = store(out, name, keys[i], malformed ? MALFORMED : VALID, true);
          conf.set(KEY, "purpose=global");
          pair(name, conf, VALID, malformed);
        }
      }
      if (passed.size() != 38 || EXPECTED.size() != 38 || !passed.containsAll(EXPECTED)) {
        throw new AssertionError("incomplete cases");
      }
      status = 0;
    } catch (Throwable failure) {
      failureClass = failure.getClass().getName();
      System.out.println("S3_CONTEXT_EQUIVALENTS_FAIL case=" + active + " class=" + failureClass);
    } finally {
      for (Path store : stores) {
        try { Files.deleteIfExists(store); }
        catch (Exception cleanup) { status = 1; failureClass = cleanup.getClass().getName(); }
      }
    }
    String json = "{\"scope\":\"Hadoop helpers and synthetic local JCEKS only; no Comet admission or S3 reader\","
        + "\"expected\":38,\"passed\":" + passed.size() + ",\"exit_code\":" + status
        + ",\"failure_class\":\"" + failureClass + "\"}";
    System.out.println("S3_CONTEXT_EQUIVALENTS_SUMMARY " + json);
    if (created) {
      try { Files.writeString(out.resolve("summary.json"), json + "\n"); }
      catch (Exception failure) { status = 1; System.out.println("S3_CONTEXT_EQUIVALENTS_FAIL case=summary class=" + failure.getClass().getName()); }
    }
    System.exit(status);
  }
}
