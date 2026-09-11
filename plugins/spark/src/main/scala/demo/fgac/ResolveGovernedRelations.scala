package demo.fgac

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule

class ResolveGovernedRelations extends Rule[LogicalPlan] {
  private val mapper = new ObjectMapper()
  private val client = HttpClient.newHttpClient()
  private val cache = scala.collection.concurrent.TrieMap.empty[String, Option[(String, String)]]

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case alias @ SubqueryAlias(identifier, child)
        if !child.isInstanceOf[GovernedRelation] && !child.isInstanceOf[GovernedRemoteSubplan] =>
      governance(identifier.name.toLowerCase) match {
        case Some((relationId, schemaVersion)) =>
          SubqueryAlias(identifier, GovernedRelation(relationId, schemaVersion, child.output))
        case None => alias
      }
  }

  private def governance(name: String): Option[(String, String)] = cache.getOrElseUpdate(name, {
    val base = sys.env.getOrElse("POLARIS_URL", "http://polaris-fgac:8181")
    val request = HttpRequest.newBuilder().uri(URI.create(s"$base/v2/relations/public/$name")).GET().build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() == 404) None
    else if (response.statusCode() / 100 != 2)
      throw new IllegalStateException(s"Catalog discovery failed: ${response.statusCode()} ${response.body()}")
    else {
      val json = mapper.readTree(response.body())
      if (!json.path("governed").asBoolean(false)) None
      else Some(json.path("relation_id").asText() -> json.path("schema_version").asText())
    }
  })
}
