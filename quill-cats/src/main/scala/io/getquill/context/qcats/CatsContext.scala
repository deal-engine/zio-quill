package io.getquill.context.qcats

import io.getquill.NamingStrategy
import io.getquill.context.{Context, ExecutionInfo, ContextVerbStream}
import cats.effect.IO
import fs2.Stream

trait CatsContext[+Idiom <: io.getquill.idiom.Idiom, +Naming <: NamingStrategy]
    extends Context[Idiom, Naming]
    with ContextVerbStream[Idiom, Naming] {

  // It's nice that we don't actually have to import any JDBC libraries to have a Connection type here
  override type StreamResult[T]         = Stream[IO, T]
  override type Result[T]               = IO[T]
  override type RunQueryResult[T]       = List[T]
  override type RunQuerySingleResult[T] = T

  // Need explicit return-type annotations due to scala/bug#8356. Otherwise macro system will not understand Result[Long]=Task[Long] etc...
  def executeQuery[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor)(
    info: ExecutionInfo,
    dc: Runner
  ): IO[List[T]]
  def executeQuerySingle[T](
    sql: String,
    prepare: Prepare = identityPrepare,
    extractor: Extractor[T] = identityExtractor
  )(info: ExecutionInfo, dc: Runner): IO[T]
}
