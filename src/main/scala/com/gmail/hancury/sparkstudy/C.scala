package com.gmail.hancury.sparkstudy

object C extends App {

  // 5. index generating syntax : to, until
  val t = 1 to 10 // less or equal than
  val u = 1 until 10 // less than

  println(t, t.getClass)
  println(u, u.getClass)

  // 6. `for` syntactic sugar : foreach, map, flatMap, filter
  val seq: Seq[Int] = Seq(1,2,3,4,5,6,7,8,9,10)

  var even: Seq[Int] =
    for {
      n <- seq // generator
      if n % 2 == 0  // filter
    } yield n * 10 // transform

  var odd: Seq[Int] =
    for {
      n <- seq // generator
      if n % 2 == 1  // filter
    } yield n * 10 // transform

  val knitList: List[String] =
    for {
      pre <- List("a","b","c") // 1rd generator
      sur <- seq.take(3) // 2nd generator
      res = pre + sur // transform
    } yield "res: " + res // transform

  println(even)
  println(odd)
  println(knitList)

  even =
    seq // generator
      .filter(n => n % 2 == 0) // filter
      .map(n => n * 10) // transform

  odd =
    seq // generator
      .filter(n => n % 2 == 1) //filter
      .map(n => n * 10) // transform

  val knitSeq: Seq[String] =
    Seq("a","b","c") // 1rd generator
      .flatMap{
        pre =>
          val ss = seq.take(3) // 2nd generator
          ss
            .map(sur => pre + sur) // transform
            .map(res => "res: " + res) // transform
          }

  println(even)
  println(odd)
  println(knitSeq)

  for(x <- 1 until 10) print(x)
  println()
  (1 until 10).foreach(x => print(x))
}
