package io.getquill.context.cats.jasync.postgres

import com.github.jasync.sql.db.{QueryResult, ResultSetKt}
import io.getquill.ReturnAction.ReturnColumns
import io.getquill.base.Spec
import io.getquill.context.cats.PostgresCatsJAsyncContext

import io.getquill.{Literal, ReturnAction}

class PostgresJAsyncContextSpec extends Spec with CatsSpec {

  import context._

  "run non-batched action" in {
    val insert = quote { (i: Int) =>
      qr1.insert(_.i -> i)
    }
    runSyncUnsafe(implicit ec => testContext.run(insert(lift(1)))) mustEqual 1
  }

  "Insert with returning with single column table" in {
    val inserted: Long = runSyncUnsafe(implicit ec =>
      testContext.run {
        qr4.insertValue(lift(TestEntity4(0))).returningGenerated(_.i)
      }
    )
    runSyncUnsafe(implicit ec => testContext.run(qr4.filter(_.i == lift(inserted)))).head.i mustBe inserted
  }
  "Insert with returning with multiple columns" in {
    runSyncUnsafe(implicit ec => testContext.run(qr1.delete))
    val inserted = runSyncUnsafe(implicit ec =>
      testContext.run {
        qr1.insertValue(lift(TestEntity("foo", 1, 18L, Some(123), true))).returning(r => (r.i, r.s, r.o))
      }
    )
    (1, "foo", Some(123)) mustBe inserted
  }

  "performIO" in {
    runSyncUnsafe(implicit ec => performIO(runIO(qr4).transactional))
  }

  "probe" in {
    probe("select 1").toOption mustBe defined
  }

  "cannot extract" in {
    object ctx extends PostgresCatsJAsyncContext(Literal, "testPostgresDB") {
      override def handleSingleResult[T](sql: String, list: List[T]) = super.handleSingleResult(sql, list)

      override def extractActionResult[O](
        returningAction: ReturnAction,
        returningExtractor: ctx.Extractor[O]
      )(result: QueryResult) =
        super.extractActionResult(returningAction, returningExtractor)(result)
    }
    intercept[IllegalStateException] {
      val v = ctx.extractActionResult(ReturnColumns(List("w/e")), (row, session) => 1)(
        new QueryResult(0, "w/e", ResultSetKt.getEMPTY_RESULT_SET)
      )
      ctx.handleSingleResult("<not used>", v)
    }
    ctx.close
  }

  "prepare" in {
    testContext.prepareParams(
      "",
      (ps, session) => (Nil, ps ++ List("Sarah", 127))
    ) mustEqual List("'Sarah'", "127")
  }

  override protected def beforeAll(): Unit = {
    runSyncUnsafe(implicit ec => testContext.run(qr1.delete))
    ()
  }
}
