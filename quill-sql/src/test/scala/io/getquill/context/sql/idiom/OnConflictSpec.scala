package io.getquill.context.sql.idiom

import io.getquill.TestEntities
import io.getquill.base.Spec
import io.getquill.context.sql.SqlContext

trait OnConflictSpec extends Spec {
  val ctx: SqlContext[_, _] with TestEntities
  import ctx._

  lazy val e = TestEntity("s1", 1, 1, None, true)

  def ins = quote(query[TestEntity].insertValue(lift(e)))
  def del = quote(query[TestEntity].delete)

  def `no target - ignore` = quote {
    ins.onConflictIgnore
  }
  def `cols target - ignore` = quote {
    ins.onConflictIgnore(_.i)
  }
  def `no target - update` = quote {
    ins.onConflictUpdate((t, e) => t.l -> (t.l + e.l) / 2, _.s -> _.s)
  }
  def `cols target - update` = quote {
    ins.onConflictUpdate(_.i, _.s)((t, e) => t.l -> (t.l + e.l) / 2, _.s -> _.s)
  }
  def insBatch = quote(liftQuery(List(e, TestEntity("s2", 1, 2L, Some(1), true))))

  def `no target - ignore batch` = quote {
    insBatch.foreach(query[TestEntity].insertValue(_).onConflictIgnore)
  }
}
