package io.getquill.context.cats.jasync.postgres

import com.github.jasync.sql.db.postgresql.PostgreSQLConnection
import io.getquill.context.sql.{TestDecoders, TestEncoders}
import io.getquill.context.cats.{
  JAsyncContextConfig,
  PostgresCatsJAsyncContext,
  PostgresJAsyncContextConfig,
  CatsJAsyncConnection
}
import io.getquill.util.LoadConfig
import io.getquill.{Literal, PostgresDialect, TestEntities}
import cats._

class TestContext
    extends PostgresCatsJAsyncContext(Literal, "testPostgresDB")
    with TestEntities
    with TestEncoders
    with TestDecoders {

  val config: JAsyncContextConfig[PostgreSQLConnection] = PostgresJAsyncContextConfig(LoadConfig("testPostgresDB"))

}
