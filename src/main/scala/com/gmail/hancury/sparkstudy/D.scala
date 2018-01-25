package com.gmail.hancury.sparkstudy

/* 7.
   `trait`, `class` creation
   `match` & `case` syntax : pattern matching
   `Exception` & `try, catch` syntax
*/
trait MyOption[+A]{
  // mandatory
  def flatten: A = this match { // official name : get
    case MySome(x) => x
    case _ => throw new Exception("MyNone : you cannot flatten this")
  }

  def map[B](f: A => B): MyOption[B] =
    this match {
      case MySome(x) =>
        try MySome(f(x))
        catch {case e: Exception => MyNone}
      case _ => MyNone
    }

  // free lunch method
  def flatMap[B](f: A => MyOption[B]): MyOption[B] =
    map(f(_).flatten)
}

// there is only two possible cases : Some, None
case object MyNone extends MyOption[Nothing]
case class MySome[A](arg: A) extends MyOption[A] // unit

object MyOption {
  def apply[A](arg: A) = MySome(arg)
  def lift[A,B](f: A => B): MyOption[A] => MyOption[B] =
    oa => oa.map(f)
}

object D extends App {
  def logTen(n: Int): Double =
    if(n < 0) throw new Exception("negative number is not allowed")
    else Math.log10(n)

  val mo1 = MyOption(-10)
  // mo1.map(logTen(_)).flatten // caution : Exception

  val mo2 = MyOption(10)
  val logTenOp = MyOption.lift(logTen)
  println(logTenOp(mo2).flatten)

  /*
  homework ->
    Either : How to conserve Exception messages?
  */

  Option(1).map(_ + 1).getOrElse(null)
}