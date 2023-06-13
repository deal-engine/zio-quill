package io.getquill.context.cats.jasync.postgres

import io.getquill.context.sql.base.OnConflictSpec

class OnConflictAsyncSpec extends OnConflictSpec with CatsSpec {

  import context._

  override protected def beforeAll(): Unit = {
    runSyncUnsafe(implicit ec => context.run(qr1.delete))
    ()
  }

  "ON CONFLICT DO NOTHING" in {
    import `onConflictIgnore`._
    runSyncUnsafe(implicit ec => context.run(testQuery1)) mustEqual res1
    runSyncUnsafe(implicit ec => context.run(testQuery2)) mustEqual res2
    runSyncUnsafe(implicit ec => context.run(testQuery3)) mustEqual res3
  }

  "ON CONFLICT (i) DO NOTHING" in {
    import `onConflictIgnore(_.i)`._
    runSyncUnsafe(implicit ec => context.run(testQuery1)) mustEqual res1
    runSyncUnsafe(implicit ec => context.run(testQuery2)) mustEqual res2
    runSyncUnsafe(implicit ec => context.run(testQuery3)) mustEqual res3
  }

  "ON CONFLICT (i) DO UPDATE ..." in {
    import `onConflictUpdate(_.i)((t, e) => ...)`._
    runSyncUnsafe(implicit ec => context.run(testQuery(e1))) mustEqual res1
    runSyncUnsafe(implicit ec => context.run(testQuery(e2))) mustEqual res2
    runSyncUnsafe(implicit ec => context.run(testQuery(e3))) mustEqual res3
    runSyncUnsafe(implicit ec => context.run(testQuery4)) mustEqual res4
  }
}
