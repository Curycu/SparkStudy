package com.gmail.hancury.sparkstudy

// 8.
//   List data structure
//   basic : push, foreach, fold, map, filter, flatMap, etc
//   application : reverse, append, repeat
//   etc : `while`, `@tailrec`, `var`, `toString`
case class Chain[+A](head: A, tail: MyList[A]) extends MyList[A] // official name `::`
case object EmptyList extends MyList[Nothing] // official name `Nil`

class MyList[+A]{

  def get = this match { // official name : `head`
    case Chain(h, _) => h
    case _ => throw new Exception("Empty List")
  }

  def push[B >: A](arg: B): MyList[B] = // official name `+:`
    Chain(arg, this)

  final def foreach(f: A => Unit): Unit =
    this match {
      case EmptyList => Unit
      case Chain(h, t) => f(h); t.foreach(f)
    }

  import scala.annotation.tailrec

  @tailrec
  final def foldLeft[B](z: B)(f: (B, A) => B): B = // official name `/:`
    this match {
      case EmptyList => z
      case Chain(h, t) => t.foldLeft(f(z, h))(f)
    }

  def reverse: MyList[A] =
    foldLeft(MyList[A]())((b, a) => Chain(a, b))

  def append[B >: A](args: MyList[B]): MyList[B] = { // official name `++`
    val o: MyList[B] = this
    args.foldLeft(o.reverse)((b, a) => Chain(a, b)).reverse
  }

  def foldRight[B](z: B)(f: (A, B) => B): B = // official name `:\`
    reverse.foldLeft(z)((a, b) => f(b, a))

  def map[B](f: A => B): MyList[B] =
    foldRight(MyList[B]())((a, b) => Chain(f(a), b))

  override def toString: String = // `print` & `println` reference `toString` method
    foldRight("")((a, b) => a + "(" + b)

  def filter(f: A => Boolean): MyList[A] =
    foldRight(MyList[A]())((a, b) => if(f(a)) b push a else b)

  def flatMap[B](f: A => MyList[B]): MyList[B] =
    foldLeft(MyList[B]())((b, a) => b append f(a))

  def repeat(n: Int): MyList[A] =
    flatMap {
      (arg: A) =>
        var res = MyList[A]()
        for(idx <-  1 to n){
          res = res push arg
        }
        res
    }
}

object MyList {
  def apply[A](args: A*): MyList[A] =
    if(args.isEmpty) EmptyList
    else Chain(args.head, apply(args.tail: _*))
}

object Fifth extends App {
  val n = MyList(1,2,3,4,5)
  val r = n.reverse
  val nm = n map (_ + 1)
  val nf = n filter (_ % 2 == 1)
  val nfm = n.flatMap(a => EmptyList push a push a.toString)
  val rp = n.repeat(3)

  println("n: " + n)
  println("r: " + r)
  println("nm: " + nm)
  println("nf: " + nf)
  println("nfm: " + nfm + " [" + nfm.get.getClass + "]")
  println("rp: " + rp)

  val summation = (arg: MyList[Int]) => arg.foldLeft(0)(_ + _) // summation
  val product = (arg: MyList[Int]) => arg.foldLeft(1)(_ * _) // product

  /*
  homework ->
    map2[A, B, C](g1: MyList[A], g2: MyList[B])(f: (A, B) => C): MyList[C]
  */

  val z = List[Int]()
  val left = List(1,2,3,4,5)./:(z)((b, a) => ::(a, b))
  val right = List(1,2,3,4,5).:\(z)((a, b) => ::(a, b))

  println(left) // foldLeft : reversed order
  println(right) // foldRight : normal order
}
