package io.getquill.context.cats

import com.github.jasync.sql.db.{ConcreteConnection, QueryResult}
import com.github.jasync.sql.db.pool.{ConnectionPool => KConnectionPool}
import cats.effect.{IO => CatsIO}

import scala.jdk.CollectionConverters._

trait CatsJAsyncConnection {
  protected def takeConnection[A, C <: ConcreteConnection]: (C => CatsIO[A]) => CatsIO[A]

  /*

      override fun <A> inTransaction(f: (Connection) -> CompletableFuture<A>): CompletableFuture<A> {
        return this.sendQuery("BEGIN").flatMapAsync(configuration.executionContext) {
            val p = CompletableFuture<A>()
            f(this).onCompleteAsync(configuration.executionContext) { ty1 ->
                sendQuery(if (ty1.isFailure) "ROLLBACK" else "COMMIT").onCompleteAsync(configuration.executionContext) { ty2 ->
                    if (ty2.isFailure && ty1.isSuccess)
                        p.failed((ty2 as Failure).exception)
                    else
                        p.complete(ty1)
                }
            }
            p
        }
    }


   */

  private[cats] final def transaction[R <: CatsJAsyncConnection, A](action: CatsIO[A]): CatsIO[A] =
    // Taken from ConcreteConnectionBase.kt to avoid usage of pool.inTransaction
    takeConnection { (conn: R) =>
      conn.sendQuery("BEGIN").flatMap { _ =>
        action.flatMap { a =>
          conn.sendQuery("COMMIT").map(_ => a)
        }.handleErrorWith { e =>
          conn.sendQuery("ROLLBACK").flatMap[A](_ => CatsIO.raiseError(e))
        }
      }
    }

  private[cats] final def sendQuery(query: String): CatsIO[QueryResult] =
    takeConnection((conn: ConcreteConnection) => CatsJAsyncConnection.sendQuery(conn, query))

  private[cats] final def sendPreparedStatement(sql: String, params: Seq[Any]): CatsIO[QueryResult] =
    takeConnection { (conn: ConcreteConnection) =>
      CatsJAsyncConnection.sendPreparedStatement(conn, sql, params)
    }

}

object CatsJAsyncConnection {

  def sendQuery[C <: ConcreteConnection](query: String): C => CatsIO[QueryResult] =
    (conn: C) => CatsIO.fromCompletableFuture(CatsIO(conn.sendQuery(query)))

  def sendPreparedStatement[C <: ConcreteConnection](
    connection: C,
    sql: String,
    params: Seq[Any]
  ): CatsIO[QueryResult] =
    CatsIO.fromCompletableFuture(CatsIO(connection.sendPreparedStatement(sql, params.asJava)))

  def sendQuery[C <: ConcreteConnection](connection: C, query: String): CatsIO[QueryResult] =
    CatsIO.fromCompletableFuture(CatsIO(connection.sendQuery(query)))

  /*
  def make[C <: ConcreteConnection](pool: KConnectionPool[C]): CatsJAsyncConnection = new CatsJAsyncConnection {

    override protected def takeConnection[A, C <: ConcreteConnection]
      : (C => CatsIO[QueryResult]) => CatsIO[QueryResult] = {
      val ret = (f: C => CatsIO[QueryResult]) =>
        CatsIO
          .fromCompletableFuture[C](CatsIO(pool.take()))
          .bracket(f)(conn => CatsIO.fromCompletableFuture(CatsIO(pool.giveBack(conn))).void)
      ret
    }

  }*/

  /*
  def make[C <: ConcreteConnection](connection: C): CatsJAsyncConnection = new CatsJAsyncConnection {
    override protected def takeConnection: (C => CatsIO[QueryResult]) => CatsIO[QueryResult] =
      (f: C => CatsIO[QueryResult]) => CatsIO.apply(connection).bracket(f)(conn => CatsIO.unit)
  }*/

  /*
  def live[C <: ConcreteConnection: Tag]: ZLayer[JAsyncContextConfig[C], Throwable, CatsJAsyncConnection] =
    ZLayer.scoped {
      for {
        env <- ZIO.environment[JAsyncContextConfig[C]]
        pool <- ZIO.acquireRelease(
                  ZIO.attempt(
                    new KConnectionPool[C](
                      env.get.connectionFactory(env.get.connectionPoolConfiguration.getConnectionConfiguration),
                      env.get.connectionPoolConfiguration
                    )
                  )
                )(pool => ZIO.fromCompletableFuture(pool.disconnect()).orDie)
      } yield (CatsJAsyncConnection.make[C](pool))
    }*/

}
