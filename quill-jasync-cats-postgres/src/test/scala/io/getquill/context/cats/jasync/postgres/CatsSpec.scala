package io.getquill.context.cats.jasync.postgres

import io.getquill.base.Spec
import io.getquill.context.cats.CatsJAsyncConnection
import org.scalatest.BeforeAndAfterAll
import fs2.Stream
import cats.effect.unsafe.implicits.global
import cats.effect.IO
import scala.concurrent.ExecutionContext

trait CatsSpec extends Spec with BeforeAndAfterAll {

  lazy val context = testContext
  val ctx          = context

  def accumulate[T](stream: Stream[IO, T]): IO[List[T]] =
    stream.compile.toList

  def collect[T](stream: Stream[IO, T]): List[T] =
    stream.compile.toList.unsafeRunSync()

  def runSyncUnsafe[T](qcats: IO[T]): T =
    qcats.unsafeRunSync()

  implicit class CatsAnyOps[T](qcats: IO[T]) {
    def runSyncUnsafe() =
      qcats.unsafeRunSync()
  }

  implicit class ZStreamTestExt[T](stream: Stream[IO, T]) {
    def runSyncUnsafe() = collect[T](stream)
  }

  implicit class CatsTestExt[T](qcats: IO[T]) {
    def runSyncUnsafe() = CatsSpec.this.runSyncUnsafe[T](qcats)
  }
}
