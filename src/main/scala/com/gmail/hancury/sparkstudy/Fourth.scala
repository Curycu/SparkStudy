package com.gmail.hancury.sparkstudy

// 7.
//   `case` syntax : pattern matching
//   `try` & `catch` syntax
trait MyOption[+A]{
  // mandatory
  def flatten: A = this match {
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

object Fourth extends App {
  def logTen(n: Int): Double =
    if(n < 0) throw new Exception("negative number is not allowed")
    else Math.log10(n)

  val mo1 = MyOption(-10)
  // mo1.map(x => logTen(x)).flatten // caution : error

  val mo2 = MyOption(10)
  val logTenOp = MyOption.lift(logTen)
  println(logTenOp(mo2).flatten)

  /*
  homework ->
    Either : How to conserve Exception messages?
  */
}