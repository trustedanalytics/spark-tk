import scala.util.Properties

val EXPECTED_SPARK_VERSION = scala.util.Properties.envOrElse("SPARK_VERSION", "")
var CURRENT_SPARK_VERSION = sc.version
if (sc.version == EXPECTED_SPARK_VERSION) {
  System.exit(0);
}
else {
  println(s"Incorrect spark version, expected $EXPECTED_SPARK_VERSION got $CURRENT_SPARK_VERSION.")
  System.exit(1);
}
