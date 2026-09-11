package demo.fgac

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.catalyst.expressions._

object RemotePlanJson {
  private val mapper = new ObjectMapper()

  def request(plan: GovernedRemoteSubplan): String = {
    var root: ObjectNode = mapper.createObjectNode()
    root.put("op", "governed_scan")
    root.put("relation", plan.relationId)

    plan.filters.foreach { filter =>
      val node = mapper.createObjectNode()
      node.put("op", "filter")
      node.set("condition", expression(filter))
      node.set("input", root)
      root = node
    }

    val project = mapper.createObjectNode()
    project.put("op", "project")
    val columns = project.putArray("columns")
    plan.output.foreach(a => columns.add(a.name))
    project.set("input", root)

    val request = mapper.createObjectNode()
    request.put("version", 2)
    request.put("schema_version", plan.schemaVersion)
    request.set("root", project)
    mapper.writeValueAsString(request)
  }

  private def expression(expr: Expression): JsonNode = expr match {
    case a: AttributeReference =>
      val n = mapper.createObjectNode(); n.put("op", "column"); n.put("name", a.name); n
    case Literal(value, dataType) =>
      val n = mapper.createObjectNode(); n.put("op", "literal")
      value match {
        case null => n.put("data_type", "null"); n.putNull("value")
        case v: Int => n.put("data_type", "long"); n.put("value", v.toLong)
        case v: Long => n.put("data_type", "long"); n.put("value", v)
        case v: Double => n.put("data_type", "double"); n.put("value", v)
        case v: Boolean => n.put("data_type", "boolean"); n.put("value", v)
        case v => n.put("data_type", "string"); n.put("value", v.toString)
      }
      n
    case EqualTo(l, r) => binary("eq", l, r)
    case Not(EqualTo(l, r)) => binary("neq", l, r)
    case GreaterThan(l, r) => binary("gt", l, r)
    case GreaterThanOrEqual(l, r) => binary("gte", l, r)
    case LessThan(l, r) => binary("lt", l, r)
    case LessThanOrEqual(l, r) => binary("lte", l, r)
    case And(l, r) => binary("and", l, r)
    case Or(l, r) => binary("or", l, r)
    case Not(child) => unary("not", child)
    case IsNull(child) => unary("is_null", child)
    case IsNotNull(child) => unary("is_not_null", child)
    case other => throw new IllegalArgumentException(s"Unsafe remote expression: ${other.sql}")
  }

  private def binary(op: String, left: Expression, right: Expression): ObjectNode = {
    val n = mapper.createObjectNode(); n.put("op", op)
    n.set("left", expression(left)); n.set("right", expression(right)); n
  }

  private def unary(op: String, child: Expression): ObjectNode = {
    val n = mapper.createObjectNode(); n.put("op", op); n.set("input", expression(child)); n
  }
}
