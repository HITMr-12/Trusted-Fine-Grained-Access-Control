package demo.fgac

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class GovernedRemoteExec(remote: GovernedRemoteSubplan) extends LeafExecNode {
  override def output: Seq[Attribute] = remote.output

  override protected def doExecute(): RDD[InternalRow] = {
    val endpoint = sys.env.getOrElse("REMOTE_URL", "http://remote:8002") + "/v2/subplans"
    val token = sys.env.getOrElse("USER_TOKEN", "alice-token")
    val body = RemotePlanJson.request(remote)
    System.err.println(s"[FGAC] submitting Catalyst subtree: $body")

    val request = HttpRequest.newBuilder()
      .uri(URI.create(endpoint))
      .header("Content-Type", "application/json")
      .header("Authorization", s"Bearer $token")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() / 100 != 2) {
      throw new IllegalStateException(s"Remote FGAC failed: ${response.statusCode()} ${response.body()}")
    }

    val mapper = new ObjectMapper()
    val json = mapper.readTree(response.body())
    val rows = json.path("inline_rows")
    if (!rows.isArray) throw new IllegalStateException("Remote response has no inline_rows")
    val converted = rows.elements()
    val buffer = scala.collection.mutable.ArrayBuffer.empty[InternalRow]
    while (converted.hasNext) {
      val row = converted.next()
      val values = output.zipWithIndex.map { case (attribute, index) =>
        convert(row.get(index), attribute.dataType)
      }
      buffer += new GenericInternalRow(values.toArray[Any])
    }
    val schema = StructType(output.map(a => StructField(a.name, a.dataType, a.nullable)))
    sparkContext
      .parallelize(buffer.toSeq, math.max(1, math.min(2, buffer.size)))
      .mapPartitions { rows =>
        val toUnsafe = UnsafeProjection.create(schema)
        rows.map(row => toUnsafe(row).copy())
      }
  }

  private def convert(value: JsonNode, dataType: DataType): Any = {
    if (value == null || value.isNull) null
    else dataType match {
      case StringType => UTF8String.fromString(value.asText())
      case IntegerType => value.asInt()
      case LongType => value.asLong()
      case DoubleType => value.asDouble()
      case FloatType => value.floatValue()
      case BooleanType => value.asBoolean()
      case other => throw new IllegalArgumentException(s"Unsupported inline type: $other")
    }
  }
}
