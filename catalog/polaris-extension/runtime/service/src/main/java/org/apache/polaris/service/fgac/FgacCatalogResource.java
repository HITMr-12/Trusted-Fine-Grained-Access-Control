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
package org.apache.polaris.service.fgac;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Minimal FGAC compatibility API hosted by the Polaris control plane. */
@ApplicationScoped
@Path("/v2")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class FgacCatalogResource {

  private static final List<String> COLUMNS =
      List.of("id", "region", "amount", "owner", "card_no");
  private static final Set<String> OPERATORS = Set.of("governed_scan", "filter", "project");

  @GET
  @Path("/relations/{namespace}/{table}")
  public Response relation(
      @PathParam("namespace") String namespace, @PathParam("table") String table) {
    if (!"public".equals(namespace)) {
      return error(Response.Status.NOT_FOUND, "Unknown namespace");
    }
    if ("departments".equals(table)) {
      return Response.ok(
              Map.of(
                  "relation_id", "local.public.departments",
                  "governed", false,
                  "schema_version", "1",
                  "access_mode", "direct",
                  "format", "csv",
                  "storage_uri", "public://departments.csv"))
          .build();
    }
    if (!"orders".equals(table)) {
      return error(Response.Status.NOT_FOUND, "Unknown relation");
    }
    return Response.ok(
            Map.of(
                "relation_id", "lake.sales.orders",
                "governed", true,
                "schema_version", "1",
                "access_mode", "remote",
                "format", "parquet",
                "storage_object", "governed.orders.v1",
                "schema",
                    List.of(
                        field(1, "id", "integer"),
                        field(2, "region", "string"),
                        field(3, "amount", "integer"),
                        field(4, "owner", "string"),
                        field(5, "card_no", "string")),
                "remote_capabilities", OPERATORS))
        .build();
  }

  @POST
  @Path("/authorize")
  public Response authorize(
      @HeaderParam("Authorization") String authorization, AuthorizationRequest request) {
    String principal = principal(authorization);
    if (principal == null) {
      return error(Response.Status.UNAUTHORIZED, "Invalid or missing bearer token");
    }
    if (request == null || !"lake.sales.orders".equals(request.relationId)) {
      return error(Response.Status.FORBIDDEN, "Relation is not governed");
    }
    if (!"1".equals(request.schemaVersion)) {
      return error(Response.Status.CONFLICT, "Schema version mismatch");
    }
    if (request.requestedOperators == null
        || !OPERATORS.containsAll(request.requestedOperators)) {
      return error(Response.Status.FORBIDDEN, "Operator is outside relation contract");
    }
    if (request.requestedColumns == null || !COLUMNS.containsAll(request.requestedColumns)) {
      return error(Response.Status.FORBIDDEN, "Column is not releasable");
    }
    String region = "alice".equals(principal) ? "CN" : "US";
    Map<String, Object> policy =
        Map.of(
            "version", "2",
            "row_filter", Map.of("column", "region", "op", "eq", "value", region),
            "masks", Map.of("card_no", Map.of("type", "last4", "prefix", "****")),
            "releasable_columns", COLUMNS);
    return Response.ok(
            Map.of(
                "principal", principal,
                "relation_id", request.relationId,
                "schema_version", "1",
                "policy", policy,
                "storage_object", "governed.orders.v1",
                "authorized_operators", OPERATORS,
                "authorized_columns", COLUMNS))
        .build();
  }

  private static Map<String, Object> field(int id, String name, String type) {
    return Map.of("id", id, "name", name, "type", type, "nullable", true);
  }

  private static String principal(String authorization) {
    if ("Bearer alice-token".equals(authorization)) {
      return "alice";
    }
    if ("Bearer bob-token".equals(authorization)) {
      return "bob";
    }
    return null;
  }

  private static Response error(Response.Status status, String detail) {
    return Response.status(status).entity(Map.of("detail", detail)).build();
  }

  public static class AuthorizationRequest {
    @JsonProperty("relation_id")
    public String relationId;

    @JsonProperty("requested_operators")
    public List<String> requestedOperators;

    @JsonProperty("requested_columns")
    public List<String> requestedColumns;

    @JsonProperty("schema_version")
    public String schemaVersion;
  }
}
