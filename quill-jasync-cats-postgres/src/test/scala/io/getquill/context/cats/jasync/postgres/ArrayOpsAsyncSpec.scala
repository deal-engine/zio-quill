package io.getquill.context.cats.jasync.postgres

import io.getquill.context.sql.base.ArrayOpsSpec

class ArrayOpsAsyncSpec extends ArrayOpsSpec with CatsSpec {
  import context._

  "contains" in {
    runSyncUnsafe(implicit ec => context.run(`contains`.`Ex 1 return all`)) mustBe `contains`.`Ex 1 expected`
    runSyncUnsafe(implicit ec => context.run(`contains`.`Ex 2 return 1`)) mustBe `contains`.`Ex 2 expected`
    runSyncUnsafe(implicit ec => context.run(`contains`.`Ex 3 return 2,3`)) mustBe `contains`.`Ex 3 expected`
    runSyncUnsafe(implicit ec => context.run(`contains`.`Ex 4 return empty`)) mustBe `contains`.`Ex 4 expected`
  }

  override protected def beforeAll(): Unit = {
    runSyncUnsafe(implicit ec => context.run(entity.delete))
    runSyncUnsafe(implicit ec => context.run(insertEntries))
    ()
  }

}
