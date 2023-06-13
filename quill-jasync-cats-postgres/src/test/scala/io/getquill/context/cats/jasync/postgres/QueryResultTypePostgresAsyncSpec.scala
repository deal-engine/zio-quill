package io.getquill.context.cats.jasync.postgres

import io.getquill.context.sql.base.QueryResultTypeSpec

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._
import scala.math.BigDecimal.int2bigDecimal

class QueryResultTypePostgresAsyncSpec extends QueryResultTypeSpec with CatsSpec {

  import context._

  val insertedProducts = new ConcurrentLinkedQueue[Product]

  override def beforeAll = {
    runSyncUnsafe(implicit ec => testContext.run(deleteAll))
    val ids = runSyncUnsafe(implicit ec => testContext.run(liftQuery(productEntries).foreach(e => productInsert(e))))
    val inserted = (ids zip productEntries).map { case (id, prod) =>
      prod.copy(id = id)
    }
    insertedProducts.addAll(inserted.asJava)
    ()
  }

  def products = insertedProducts.asScala.toList

  "return list" - {
    "select" in {
      runSyncUnsafe(implicit ec => testContext.run(selectAll)) must contain theSameElementsAs (products)
    }
    "map" in {
      runSyncUnsafe(implicit ec => testContext.run(map)) must contain theSameElementsAs (products.map(_.id))
    }
    "filter" in {
      runSyncUnsafe(implicit ec => testContext.run(filter)) must contain theSameElementsAs (products)
    }
    "withFilter" in {
      runSyncUnsafe(implicit ec => testContext.run(withFilter)) must contain theSameElementsAs (products)
    }
    "sortBy" in {
      runSyncUnsafe(implicit ec => testContext.run(sortBy)) must contain theSameElementsInOrderAs (products)
    }
    "take" in {
      runSyncUnsafe(implicit ec => testContext.run(take)) must contain theSameElementsAs (products)
    }
    "drop" in {
      runSyncUnsafe(implicit ec => testContext.run(drop)) must contain theSameElementsAs (products.drop(1))
    }
    "++" in {
      runSyncUnsafe(implicit ec => testContext.run(`++`)) must contain theSameElementsAs (products ++ products)
    }
    "unionAll" in {
      runSyncUnsafe(implicit ec => testContext.run(unionAll)) must contain theSameElementsAs (products ++ products)
    }
    "union" in {
      runSyncUnsafe(implicit ec => testContext.run(union)) must contain theSameElementsAs (products)
    }
    "join" in {
      runSyncUnsafe(implicit ec => testContext.run(join)) must contain theSameElementsAs (products zip products)
    }
    "distinct" in {
      runSyncUnsafe(implicit ec => testContext.run(distinct)) must contain theSameElementsAs (products
        .map(_.id)
        .distinct)
    }
  }

  "return single result" - {
    "min" - {
      "some" in {
        runSyncUnsafe(implicit ec => testContext.run(minExists)) mustEqual Some(products.map(_.sku).min)
      }
      "none" in {
        runSyncUnsafe(implicit ec => testContext.run(minNonExists)) mustBe None
      }
    }
    "max" - {
      "some" in {
        runSyncUnsafe(implicit ec => testContext.run(maxExists)) mustBe Some(products.map(_.sku).max)
      }
      "none" in {
        runSyncUnsafe(implicit ec => testContext.run(maxNonExists)) mustBe None
      }
    }
    "avg" - {
      "some" in {
        runSyncUnsafe(implicit ec => testContext.run(avgExists)) mustBe Some(
          BigDecimal(products.map(_.sku).sum) / products.size
        )
      }
      "none" in {
        runSyncUnsafe(implicit ec => testContext.run(avgNonExists)) mustBe None
      }
    }
    "size" in {
      runSyncUnsafe(implicit ec => testContext.run(productSize)) mustEqual products.size
    }
    "parametrized size" in {
      runSyncUnsafe(implicit ec => testContext.run(parametrizedSize(lift(10000)))) mustEqual 0
    }
    "nonEmpty" in {
      runSyncUnsafe(implicit ec => testContext.run(nonEmpty)) mustEqual true
    }
    "isEmpty" in {
      runSyncUnsafe(implicit ec => testContext.run(isEmpty)) mustEqual false
    }
  }
}
