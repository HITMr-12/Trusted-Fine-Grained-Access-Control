package demo.fgac

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

class GovernedPushdownRule extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val split = plan.transformUp {
      case Filter(condition, join @ Join(left, right, _, _, _)) =>
        val predicates = splitConjunctivePredicates(condition)
        val (leftPush, afterLeft) = predicates.partition { p =>
          p.references.nonEmpty && p.references.subsetOf(left.outputSet) && containsGoverned(left)
        }
        val (rightPush, remain) = afterLeft.partition { p =>
          p.references.nonEmpty && p.references.subsetOf(right.outputSet) && containsGoverned(right)
        }
        if (leftPush.isEmpty && rightPush.isEmpty) {
          Filter(condition, join)
        } else {
          val newLeft = if (leftPush.nonEmpty) Filter(leftPush.reduce(And), left) else left
          val newRight = if (rightPush.nonEmpty) Filter(rightPush.reduce(And), right) else right
          val newJoin = join.copy(left = newLeft, right = newRight)
          if (remain.nonEmpty) Filter(remain.reduce(And), newJoin) else newJoin
        }
    }

    split.transformUp {
      case Filter(condition, relation: GovernedRelation)
          if SafeExpressionChecker.isSafe(condition) =>
        GovernedRemoteSubplan(relation.relationId, relation.schemaVersion, Seq(condition), relation.output)

      case Filter(condition, remote: GovernedRemoteSubplan)
          if SafeExpressionChecker.isSafe(condition) =>
        remote.copy(filters = remote.filters :+ condition)

      case Project(projectList, relation: GovernedRelation)
          if simpleProjection(projectList) =>
        GovernedRemoteSubplan(
          relation.relationId,
          relation.schemaVersion,
          Seq.empty,
          projectList.map(_.toAttribute)
        )

      case Project(projectList, remote: GovernedRemoteSubplan)
          if simpleProjection(projectList) =>
        remote.copy(output = projectList.map(_.toAttribute))
    }
  }

  private def containsGoverned(plan: LogicalPlan): Boolean =
    plan.exists(p => p.isInstanceOf[GovernedRelation] || p.isInstanceOf[GovernedRemoteSubplan])

  private def simpleProjection(expressions: Seq[NamedExpression]): Boolean =
    expressions.forall {
      case _: AttributeReference => true
      case Alias(_: AttributeReference, _) => true
      case _ => false
    }
}

object SafeExpressionChecker {
  def isSafe(expression: Expression): Boolean = expression match {
    case _: AttributeReference | _: Literal => true
    case e: BinaryComparison => isSafe(e.left) && isSafe(e.right)
    case And(left, right) => isSafe(left) && isSafe(right)
    case Or(left, right) => isSafe(left) && isSafe(right)
    case Not(child) => isSafe(child)
    case IsNull(child) => isSafe(child)
    case IsNotNull(child) => isSafe(child)
    case _ => false
  }
}
