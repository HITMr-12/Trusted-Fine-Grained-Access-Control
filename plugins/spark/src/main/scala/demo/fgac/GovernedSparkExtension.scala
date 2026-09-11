package demo.fgac

import org.apache.spark.sql.SparkSessionExtensions

class GovernedSparkExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPostHocResolutionRule { _ => new ResolveGovernedRelations }
    extensions.injectOptimizerRule { _ => new GovernedPushdownRule }
    extensions.injectPlannerStrategy { _ => new GovernedRemoteStrategy }
  }
}
