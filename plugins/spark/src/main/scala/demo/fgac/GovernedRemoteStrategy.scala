package demo.fgac

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

class GovernedRemoteStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case remote: GovernedRemoteSubplan => GovernedRemoteExec(remote) :: Nil
    case relation: GovernedRelation =>
      GovernedRemoteExec(
        GovernedRemoteSubplan(relation.relationId, relation.schemaVersion, Seq.empty, relation.output)
      ) :: Nil
    case _ => Nil
  }
}
