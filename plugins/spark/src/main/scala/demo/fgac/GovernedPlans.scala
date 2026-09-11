package demo.fgac

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}

case class GovernedRelation(
    relationId: String,
    schemaVersion: String,
    output: Seq[Attribute]
) extends LeafNode {
  override def computeStats(): Statistics =
    Statistics(sizeInBytes = BigInt(1))
}

case class GovernedRemoteSubplan(
    relationId: String,
    schemaVersion: String,
    filters: Seq[Expression],
    output: Seq[Attribute]
) extends LeafNode {
  override def computeStats(): Statistics =
    Statistics(sizeInBytes = BigInt(1))
}
