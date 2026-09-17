/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
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
package org.apache.polaris.fgac;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Dependency-free deployment profile for the Polaris FGAC compatibility API.
 *  2026-09-16: add governed relation lake.sales.store_sales (TPC-DS SF=10 bench).
 *  Existing lake.sales.orders behavior unchanged. */
public final class MinimalPolarisFgac {
  private static final String COLUMNS = "[\"VendorID\",\"trip_distance\",\"fare_amount\",\"payment_type\"]";
  private static final String SS_COLUMNS = "[\"ss_item_sk\",\"ss_store_sk\",\"ss_quantity\",\"ss_sales_price\",\"ss_net_paid\"]";
  private static final Pattern SCHEMA = Pattern.compile("\\\"schema_version\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");

  private MinimalPolarisFgac() {}

  public static void main(String[] args) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("172.168.22.23", 18184), 0);
    server.createContext("/q/health", exchange -> send(exchange, 200, "{\"status\":\"UP\"}"));
    server.createContext("/v2/relations/public/departments", exchange -> send(exchange, 200,
        "{\"relation_id\":\"local.public.departments\",\"governed\":false,"
            + "\"schema_version\":\"1\",\"access_mode\":\"direct\",\"format\":\"csv\","
            + "\"storage_uri\":\"public://departments.csv\"}"));
    server.createContext("/v2/relations/public/orders", exchange -> send(exchange, 200,
        "{\"relation_id\":\"lake.sales.orders\",\"governed\":true,"
            + "\"schema_version\":\"1\",\"access_mode\":\"remote\",\"format\":\"parquet\","
            + "\"storage_object\":\"governed.orders.v1\",\"remote_capabilities\":"
            + "[\"governed_scan\",\"filter\",\"project\"]}"));
    server.createContext("/v2/relations/public/store_sales", exchange -> send(exchange, 200,
        "{\"relation_id\":\"lake.sales.store_sales\",\"governed\":true,"
            + "\"schema_version\":\"1\",\"access_mode\":\"remote\",\"format\":\"parquet\","
            + "\"storage_object\":\"governed.store_sales.v1\",\"remote_capabilities\":"
            + "[\"governed_scan\",\"filter\",\"project\"]}"));
    server.createContext("/v2/authorize", MinimalPolarisFgac::authorize);
    server.setExecutor(null);
    server.start();
    System.out.println("Minimal Polaris FGAC listening on :18184");
  }

  private static void authorize(HttpExchange exchange) throws IOException {
    if (!"POST".equals(exchange.getRequestMethod())) {
      send(exchange, 405, error("Method not allowed"));
      return;
    }
    String token = exchange.getRequestHeaders().getFirst("Authorization");
    String principal;
    if ("Bearer alice-token".equals(token)) {
      principal = "alice";
    } else if ("Bearer bob-token".equals(token)) {
      principal = "bob";
    } else if ("Bearer bench_full-token".equals(token)) {
      principal = "bench_full";
    } else {
      send(exchange, 401, error("Invalid or missing bearer token"));
      return;
    }
    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    boolean isOrders = body.contains("\"lake.sales.orders\"");
    boolean isStoreSales = body.contains("\"lake.sales.store_sales\"");
    if (!isOrders && !isStoreSales) {
      send(exchange, 403, error("Relation is not governed"));
      return;
    }
    Matcher schema = SCHEMA.matcher(body);
    if (!schema.find() || !"1".equals(schema.group(1))) {
      send(exchange, 409, error("Schema version mismatch"));
      return;
    }
    if (body.contains("export_raw")) {
      send(exchange, 403, error("Operator is outside relation contract"));
      return;
    }
    if (isOrders) {
      authorizeOrders(exchange, principal, body);
    } else {
      authorizeStoreSales(exchange, principal, body);
    }
  }

  private static void authorizeOrders(HttpExchange exchange, String principal, String body)
      throws IOException {
    String vendorId;
    if ("alice".equals(principal)) {
      vendorId = "1";
    } else if ("bob".equals(principal)) {
      vendorId = "2";
    } else {
      vendorId = "all";
    }
    if (body.contains("secret_payload")) {
      send(exchange, 403, error("Column is not releasable"));
      return;
    }
    String response = "{\"principal\":\"" + principal + "\","
        + "\"relation_id\":\"lake.sales.orders\",\"schema_version\":\"1\","
        + "\"storage_object\":\"governed.orders.v1\","
        + "\"authorized_operators\":[\"governed_scan\",\"filter\",\"project\"],"
        + "\"authorized_columns\":" + COLUMNS + ",\"policy\":{\"version\":\"2\","
        + ("all".equals(vendorId) ? "\"row_filter\":{\"op\":\"all\"}," : "\"row_filter\":{\"column\":\"VendorID\",\"op\":\"eq\",\"value\":" + vendorId + "},")
        + "\"masks\":{},"
        + "\"releasable_columns\":" + COLUMNS + "}}";
    send(exchange, 200, response);
  }

  private static void authorizeStoreSales(HttpExchange exchange, String principal, String body)
      throws IOException {
    if (body.contains("ss_ticket_number")) {
      send(exchange, 403, error("Column is not releasable"));
      return;
    }
    String storeSk;
    if ("alice".equals(principal)) {
      storeSk = "1";
    } else if ("bob".equals(principal)) {
      storeSk = "2";
    } else {
      storeSk = "all";
    }
    String response = "{\"principal\":\"" + principal + "\","
        + "\"relation_id\":\"lake.sales.store_sales\",\"schema_version\":\"1\","
        + "\"storage_object\":\"governed.store_sales.v1\","
        + "\"authorized_operators\":[\"governed_scan\",\"filter\",\"project\"],"
        + "\"authorized_columns\":" + SS_COLUMNS + ",\"policy\":{\"version\":\"3\","
        + ("all".equals(storeSk) ? "\"row_filter\":{\"op\":\"all\"}," : "\"row_filter\":{\"column\":\"ss_store_sk\",\"op\":\"eq\",\"value\":" + storeSk + "},")
        + "\"masks\":{},"
        + "\"releasable_columns\":" + SS_COLUMNS + "}}";
    send(exchange, 200, response);
  }

  private static String error(String detail) {
    return "{\"detail\":\"" + detail + "\"}";
  }

  private static void send(HttpExchange exchange, int status, String body) throws IOException {
    byte[] payload = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, payload.length);
    exchange.getResponseBody().write(payload);
    exchange.close();
  }
}
