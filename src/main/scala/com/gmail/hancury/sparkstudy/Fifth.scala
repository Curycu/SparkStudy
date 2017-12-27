package com.gmail.hancury.sparkstudy

class MyList[+A]{
  def foldLeft[B](z: B)(f: (B, A) => B): B = // official name `/:`
    this match {
      case EmptyList => z
      case Chain(h, t) => t.foldLeft(f(z, h))(f)
    }

  def reverse: MyList[A] = foldLeft(MyList[A]())((b, a) => Chain(a, b))

  def foldRight[B](z: B)(f: (A, B) => B): B = reverse.foldLeft(z)((a, b) => f(b, a)) // official name `:\`

  def push[B >: A](arg: B): MyList[B] = Chain(arg, this) // official name `+:`

  def append[B >: A](args: MyList[B]): MyList[B] = { // official name `++`
    val o: MyList[B] = this
    args.foldLeft(o.reverse)((b, a) => Chain(a, b)).reverse
  }
}

case class Chain[+A](head: A, tail: MyList[A]) extends MyList[A] // official name `::`
case object EmptyList extends MyList[Nothing] // official name `Nil`

object MyList {
  def apply[A](args: A*): MyList[A] =
    if(args.isEmpty) EmptyList
    else Chain(args.head, apply(args.tail: _*))
}

object Fifth extends App {
  val n = MyList(1,2,3,4,5)
  val summation = n.foldLeft(0)(_ + _) // summation
  val product = n.foldLeft(1)(_ * _) // product
  val r = n.reverse

  println(n)
  println(summation)
  println(product)
  println(r)
  println(n.append(r))

  /*
  homework ->
    map2[A, B, C](g1: MyList[A], g2: MyList[B])(f: (A, B) => C): MyList[C]
  */

  List(1,2,3).foldRight(0)(_ + _)
}
