package com.gmail.hancury.sparkstudy

class MyList[+A]{

  def foldLeft[B >: A](z: B)(f: (B, A) => B): B =
    this match {
      case EmptyList => z
      case Chain(h, t) => t.foldLeft(f(z, h))(f)
    }

  def push[B >: A](arg: B): MyList[B] = Chain(arg, this)
}

case class Chain[+A](head: A, tail: MyList[A]) extends MyList[A]
case object EmptyList extends MyList[Nothing]

object MyList {
  def apply[A](args: A*): MyList[A] =
    if(args.isEmpty) EmptyList
    else Chain(args.head, apply(args.tail: _*))
}

object Fifth extends App {
  val n = MyList(1,2,3,4)
  val sum = n.foldLeft(0)(_ + _) // summation
  val prod = n.foldLeft(1)(_ * _) // product

  println(n)
  println(sum)
  println(prod)

  List(1,2,3).foldRight(0)(_ + _) // `/:` == `foldLeft`, `:\` == `foldRight`
}
