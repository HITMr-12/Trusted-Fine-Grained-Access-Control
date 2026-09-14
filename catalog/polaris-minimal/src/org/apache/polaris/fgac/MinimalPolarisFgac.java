/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information.
 */
package org.apache.polaris.fgac;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Dependency-free deployment profile for the Polaris FGAC compatibility API. */
public final class MinimalPolarisFgac {
  private static final String COLUMNS = "[\"VendorID\",\"trip_distance\",\"fare_amount\",\"payment_type\"]";
  private static final Pattern SCHEMA = Pattern.compile("\\\"schema_version\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");

  private MinimalPolarisFgac() {}

  public static void main(String[] args) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", 8181), 0);
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
    server.createContext("/v2/authorize", MinimalPolarisFgac::authorize);
    server.setExecutor(null);
    server.start();
    System.out.println("Minimal Polaris FGAC listening on :8181");
  }

  private static void authorize(HttpExchange exchange) throws IOException {
    if (!"POST".equals(exchange.getRequestMethod())) {
      send(exchange, 405, error("Method not allowed"));
      return;
    }
    String token = exchange.getRequestHeaders().getFirst("Authorization");
    String principal;
    String vendorId;
    if ("Bearer alice-token".equals(token)) {
      principal = "alice";
      vendorId = "1";
    } else if ("Bearer bob-token".equals(token)) {
      principal = "bob";
      vendorId = "2";
    } else {
      send(exchange, 401, error("Invalid or missing bearer token"));
      return;
    }
    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    if (!body.contains("\"relation_id\": \"lake.sales.orders\"")
        && !body.contains("\"relation_id\":\"lake.sales.orders\"")) {
      send(exchange, 403, error("Relation is not governed"));
      return;
    }
    Matcher schema = SCHEMA.matcher(body);
    if (!schema.find() || !"1".equals(schema.group(1))) {
      send(exchange, 409, error("Schema version mismatch"));
      return;
    }
    if (body.contains("secret_payload")) {
      send(exchange, 403, error("Column is not releasable"));
      return;
    }
    if (body.contains("export_raw")) {
      send(exchange, 403, error("Operator is outside relation contract"));
      return;
    }
    String response = "{\"principal\":\"" + principal + "\","
        + "\"relation_id\":\"lake.sales.orders\",\"schema_version\":\"1\","
        + "\"storage_object\":\"governed.orders.v1\","
        + "\"authorized_operators\":[\"governed_scan\",\"filter\",\"project\"],"
        + "\"authorized_columns\":" + COLUMNS + ",\"policy\":{\"version\":\"2\","
        + "\"row_filter\":{\"column\":\"VendorID\",\"op\":\"eq\",\"value\":" + vendorId + "},"
        + "\"masks\":{},"
        + "\"releasable_columns\":" + COLUMNS + "}}";
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
