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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class FgacCatalogResourceTest {
  private final FgacCatalogResource resource = new FgacCatalogResource();

  @Test
  void governedRelationUsesRemoteAccess() {
    var response = resource.relation("public", "orders");
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity().toString()).contains("access_mode=remote");
  }

  @Test
  void authorizationRejectsUnapprovedColumn() {
    var request = new FgacCatalogResource.AuthorizationRequest();
    request.relationId = "lake.sales.orders";
    request.schemaVersion = "1";
    request.requestedOperators = List.of("governed_scan", "project");
    request.requestedColumns = List.of("secret_payload");
    assertThat(resource.authorize("Bearer alice-token", request).getStatus()).isEqualTo(403);
  }

  @Test
  void authorizationBindsPrincipalPolicy() {
    var request = new FgacCatalogResource.AuthorizationRequest();
    request.relationId = "lake.sales.orders";
    request.schemaVersion = "1";
    request.requestedOperators = List.of("governed_scan", "project");
    request.requestedColumns = List.of("owner", "amount");
    var response = resource.authorize("Bearer bob-token", request);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity().toString()).contains("principal=bob", "value=US");
  }
}
