package io.getquill.context.cats

import com.github.jasync.sql.db.{ConcreteConnection, Connection, QueryResult, RowData}
import com.github.jasync.sql.db.pool.ConnectionPool

import io.getquill.context.sql.SqlContext
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.context.{Context, ContextVerbTranslate, ExecutionInfo}
import io.getquill.util.ContextLogger
import io.getquill.{NamingStrategy, ReturnAction}
import kotlin.jvm.functions.Function1
import scala.concurrent.ExecutionContext
import cats.effect.{IO => CatsIO}
import cats.effect.std.AtomicCell
import scala.concurrent.ExecutionContext

import java.time.ZoneId
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.Try

abstract class CatsJAsyncContext[D <: SqlIdiom, +N <: NamingStrategy, C <: ConcreteConnection](
  val idiom: D,
  val naming: N,
  pool: ConnectionPool[C]
) extends Context[D, N]
    with ContextVerbTranslate
    with SqlContext[D, N]
    with Decoders
    with Encoders
    with CatsIOMonad {

  protected val dateTimeZone = ZoneId.systemDefault()

  private val logger = ContextLogger(classOf[CatsJAsyncContext[_, _, _]])

  override type PrepareRow = Seq[Any]
  override type ResultRow  = RowData
  override type Session    = Unit

  override type Result[T]                        = CatsIO[T]
  override type RunQueryResult[T]                = Seq[T]
  override type RunQuerySingleResult[T]          = T
  override type RunActionResult                  = Long
  override type RunActionReturningResult[T]      = T
  override type RunBatchActionResult             = Seq[Long]
  override type RunBatchActionReturningResult[T] = Seq[T]
  override type DecoderSqlType                   = SqlTypes.SqlTypes
  type DatasourceContext                         = Unit
  type Runner                                    = Unit

  override type NullChecker = CatsJasyncNullChecker
  class CatsJasyncNullChecker extends BaseNullChecker {
    override def apply(index: Int, row: RowData): Boolean =
      row.get(index) == null
  }
  implicit val nullChecker: NullChecker = new CatsJasyncNullChecker()

  implicit def toKotlinFunction[T, R](f: T => R): Function1[T, R] = new Function1[T, R] {
    override def invoke(t: T): R = f(t)
  }

  override def close =
    // Await.result(pool.disconnect(), Duration.Inf) // TO CHANGE
    ()

  protected def withConnection[T](f: Connection => CatsIO[T])(implicit ec: CatsIO[ExecutionContext]) =
    ec.flatMap {
      case TransactionalExecutionContext(ec, conn) => f(conn)
      case other                                   => f(pool)
    }

  protected def extractActionResult[O](returningAction: ReturnAction, extractor: Extractor[O])(
    result: QueryResult
  ): List[O]

  protected def expandAction(sql: String, returningAction: ReturnAction) = sql

  def probe(sql: String): Try[_] =
    Try {
      // Await.result(pool.sendQuery(sql), Duration.Inf) TODO
    }

  def transaction[T](
    f: CatsIO[ExecutionContext] => CatsIO[T]
  )(implicit ec: CatsIO[ExecutionContext] = CatsIO.executionContext) =
    ec.flatMap { implicit ec =>
      cats.effect.std.Dispatcher.parallel[CatsIO].use { dispatcher =>
        CatsIO.fromCompletableFuture {
          CatsIO {
            pool.inTransaction({ c: Connection =>
              dispatcher.unsafeToCompletableFuture(f(CatsIO(TransactionalExecutionContext(ec, c))))
            })
          }
        }
      }
    }

  def executeQuery[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor)(
    info: ExecutionInfo,
    dc: Runner
  )(implicit ec: CatsIO[ExecutionContext] = CatsIO.executionContext): CatsIO[List[T]] = {
    val (params, values) = prepare(Nil, ())
    logger.logQuery(sql, params)
    withConnection(a => CatsIO.fromCompletableFuture(CatsIO(a.sendPreparedStatement(sql, values.asJava))))
      .map(_.getRows.asScala.iterator.map(row => extractor(row, ())).toList)
  }

  def executeQuerySingle[T](
    sql: String,
    prepare: Prepare = identityPrepare,
    extractor: Extractor[T] = identityExtractor
  )(info: ExecutionInfo, dc: Runner)(implicit ec: CatsIO[ExecutionContext] = CatsIO.executionContext): CatsIO[T] =
    executeQuery(sql, prepare, extractor)(info, dc).map(handleSingleResult(sql, _))

  def executeAction(sql: String, prepare: Prepare = identityPrepare)(info: ExecutionInfo, dc: Runner)(implicit
    ec: CatsIO[ExecutionContext] = CatsIO.executionContext
  ): CatsIO[Long] = {
    val (params, values) = prepare(Nil, ())
    logger.logQuery(sql, params)
    withConnection(a => CatsIO.fromCompletableFuture(CatsIO(a.sendPreparedStatement(sql, values.asJava))))
      .map(_.getRowsAffected)
  }

  def executeActionReturning[T](
    sql: String,
    prepare: Prepare = identityPrepare,
    extractor: Extractor[T],
    returningAction: ReturnAction
  )(info: ExecutionInfo, dc: Runner)(implicit ec: CatsIO[ExecutionContext] = CatsIO.executionContext): CatsIO[T] =
    executeActionReturningMany[T](sql, prepare, extractor, returningAction)(info, dc).map(handleSingleResult(sql, _))

  def executeActionReturningMany[T](
    sql: String,
    prepare: Prepare = identityPrepare,
    extractor: Extractor[T],
    returningAction: ReturnAction
  )(info: ExecutionInfo, dc: Runner)(implicit
    ec: CatsIO[ExecutionContext] = CatsIO.executionContext
  ): CatsIO[List[T]] = {
    val expanded         = expandAction(sql, returningAction)
    val (params, values) = prepare(Nil, ())
    logger.logQuery(sql, params)
    withConnection(a => CatsIO.fromCompletableFuture(CatsIO(a.sendPreparedStatement(expanded, values.asJava))))
      .map(extractActionResult(returningAction, extractor))
  }

  def executeBatchAction(
    groups: List[BatchGroup]
  )(info: ExecutionInfo, dc: Runner)(implicit
    ec: CatsIO[ExecutionContext] = CatsIO.executionContext
  ): CatsIO[List[Long]] =
    fs2.Stream.emits {
      groups.map { case BatchGroup(sql, prepare) =>
        prepare
          .foldLeft(CatsIO.pure(List.newBuilder[Long])) { case (acc, prepare) =>
            acc.flatMap { list =>
              executeAction(sql, prepare)(info, dc).map(list += _)
            }
          }
          .map(_.result())
      }
    }.parEvalMapUnbounded(identity).compile.toList.map(_.flatten)

  def executeBatchActionReturning[T](
    groups: List[BatchGroupReturning],
    extractor: Extractor[T]
  )(info: ExecutionInfo, dc: Runner)(implicit ec: CatsIO[ExecutionContext] = CatsIO.executionContext): CatsIO[List[T]] =
    fs2.Stream.emits {
      groups.map { case BatchGroupReturning(sql, column, prepare) =>
        prepare
          .foldLeft(CatsIO.pure(List.newBuilder[T])) { case (acc, prepare) =>
            acc.flatMap { list =>
              executeActionReturning(sql, prepare, extractor, column)(info, dc).map(list += _)
            }
          }
          .map(_.result())
      }
    }.parEvalMapUnbounded(identity).compile.toList.map(_.flatten)

  override private[getquill] def prepareParams(statement: String, prepare: Prepare): Seq[String] =
    prepare(Nil, ())._2.map(prepareParam)

}
