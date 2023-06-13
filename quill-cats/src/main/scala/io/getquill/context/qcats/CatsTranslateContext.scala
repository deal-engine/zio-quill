package io.getquill.context.qcats

import io.getquill.NamingStrategy
import io.getquill.context.{Context, ContextTranslateMacro}
import io.getquill.idiom.Idiom
import cats.effect.IO
import fs2.Stream

trait CatsTranslateContext extends ContextTranslateMacro {
  this: Context[_ <: Idiom, _ <: NamingStrategy] =>

  override type TranslateResult[T] = IO[T]
  override def wrap[T](t: => T): TranslateResult[T]                                  = IO.apply(t)
  override def push[A, B](result: TranslateResult[A])(f: A => B): TranslateResult[B] = result.map(f)
  override def seq[A](list: List[TranslateResult[A]]): TranslateResult[List[A]] =
    Stream.emits(list).evalMap(identity(_)).compile.toList
}
