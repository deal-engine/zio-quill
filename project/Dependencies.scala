import sbt.*
import sbt.Keys.*

object Version {
  val zio = "2.0.15"
  val catsEffect = "3.5.0"
  val fs2 = "3.7.0"
}

sealed trait ExcludeTests
object ExcludeTests {
  case object Exclude                extends ExcludeTests
  case object Include                extends ExcludeTests
  case class KeepSome(regex: String) extends ExcludeTests
}
