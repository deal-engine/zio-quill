package io.getquill.context.cats.jasync.postgres

import io.getquill.context.sql.ProductSpec
import io.getquill.context.sql.Id

class ProductPostgresAsyncSpec extends ProductSpec with CatsSpec {

  import context._

  override def beforeAll = {
    runSyncUnsafe(implicit ec => testContext.run(quote(query[Product].delete)))
    ()
  }

  "Product" - {
    "Insert multiple products" in {
      val inserted =
        runSyncUnsafe(implicit ec =>
          fs2.Stream
            .emits(productEntries)
            .evalMap(product => testContext.run(productInsert(lift(product))))
            .compile
            .toList
        )
      val product = runSyncUnsafe(implicit ec => testContext.run(productById(lift(inserted(2))))).head
      product.description mustEqual productEntries(2).description
      product.id mustEqual inserted(2)
    }
    "Single insert product" in {
      val inserted = runSyncUnsafe(implicit ec => testContext.run(productSingleInsert))
      val product  = runSyncUnsafe(implicit ec => testContext.run(productById(lift(inserted)))).head
      product.description mustEqual "Window"
      product.id mustEqual inserted
    }

    "Single insert with inlined free variable" in {
      val prd = Product(0L, "test1", 1L)
      val inserted = runSyncUnsafe { implicit ec =>
        testContext.run {
          product.insert(_.sku -> lift(prd.sku), _.description -> lift(prd.description)).returning(_.id)
        }
      }
      val returnedProduct = runSyncUnsafe(implicit ec => testContext.run(productById(lift(inserted)))).head
      returnedProduct.description mustEqual "test1"
      returnedProduct.sku mustEqual 1L
      returnedProduct.id mustEqual inserted
    }

    "Single insert with free variable and explicit quotation" in {
      val prd = Product(0L, "test2", 2L)
      val q1 = quote {
        product.insert(_.sku -> lift(prd.sku), _.description -> lift(prd.description)).returning(_.id)
      }
      val inserted        = runSyncUnsafe(implicit ec => testContext.run(q1))
      val returnedProduct = runSyncUnsafe(implicit ec => testContext.run(productById(lift(inserted)))).head
      returnedProduct.description mustEqual "test2"
      returnedProduct.sku mustEqual 2L
      returnedProduct.id mustEqual inserted
    }

    "Single product insert with a method quotation" in {
      val prd             = Product(0L, "test3", 3L)
      val inserted        = runSyncUnsafe(implicit ec => testContext.run(productInsert(lift(prd))))
      val returnedProduct = runSyncUnsafe(implicit ec => testContext.run(productById(lift(inserted)))).head
      returnedProduct.description mustEqual "test3"
      returnedProduct.sku mustEqual 3L
      returnedProduct.id mustEqual inserted
    }

    "Single insert with value class" in {
      case class Product(id: Id, description: String, sku: Long)
      val prd = Product(Id(0L), "test2", 2L)
      val q1 = quote {
        query[Product].insert(_.sku -> lift(prd.sku), _.description -> lift(prd.description)).returning(_.id)
      }
      runSyncUnsafe(implicit ec => testContext.run(q1)) mustBe a[Id]
    }

    "supports casts from string to number" - {
      "toInt" in {
        case class Product(id: Long, description: String, sku: Int)
        val queried = runSyncUnsafe { implicit ec =>
          testContext.run {
            query[Product].filter(_.sku == lift("1004").toInt)
          }
        }.head
        queried.sku mustEqual 1004L
      }
      "toLong" in {
        val queried = runSyncUnsafe { implicit ec =>
          testContext.run {
            query[Product].filter(_.sku == lift("1004").toLong)
          }
        }.head
        queried.sku mustEqual 1004L
      }
    }
  }
}
