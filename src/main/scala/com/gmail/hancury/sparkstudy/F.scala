/*
Zeppelin notebook

# DataFrame A
==========================================
val cuUsers = sqlContext.sql("""
select
  idx,
  birth_date,
  gender
from dump.tb_user
""")

cuUsers.printSchema
==========================================
cuUsers: org.apache.spark.sql.DataFrame = [idx: int, birth_date: string, gender: string]
root
 |-- idx: integer (nullable = true)
 |-- birth_date: string (nullable = true)
 |-- gender: string (nullable = true)


# UDF (User Defined Function) : birth_date -> age
==========================================
val toAge: String => Int = // based on 2018 year
    (birth_date: String) => {
      val birth_year = birth_date.substring(0, 2).toInt
      if(birth_year <= 18) { 18 - birth_year } else { 100 - birth_year + 18 } + 1
    }

val toAgeUdf = udf[Int, String](toAge)
==========================================
toAge: String => Int = <function1>
toAgeUdf: org.apache.spark.sql.UserDefinedFunction = UserDefinedFunction(<function1>,IntegerType,List(StringType))


# apply UDF & DF -> DS
==========================================
val cuUsersWithAge =
  cuUsers
    .filter("birth_date != ''")
    .filter("gender != ''")
    .withColumn("age", toAgeUdf($"birth_date")) // mutate birth_date column with toAge function & rename as "age"
    .as[(Int, String, String, Int)] // from DataFrame to Dataset

cuUsersWithAge.printSchema
==========================================
cuUsersWithAge: org.apache.spark.sql.Dataset[(Int, String, String, Int)] = [_1: int, _2: string, _3: string, _4: int]
root
 |-- idx: integer (nullable = true)
 |-- birth_date: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: integer (nullable = true)


filtering DataFame A
==========================================
cuUsersWithAge
  .filter(u => u._3 == "F" && u._4 % 2 == 0)
  .show
==========================================



# join DataFrame B
==========================================
val targetUsers =
  sqlContext
  .sql("select idx, id from dump.tb_user where id in ('curycu')")
  .as[(Int, String)]

val joined =
  cuUsersWithAge.toDF().as("DF_A")
  .join(targetUsers.toDF().as("DF_B"), $"DF_A.idx" === $"DF_B.idx")
  .withColumn("u_id", $"DF_B.id")
  .select("DF_A.idx", "DF_A.birth_date", "DF_A.gender", "DF_A.age", "u_id")
  .orderBy(desc("DF_A.idx"))

joined.show()
joined.printSchema
==========================================
targetUsers: org.apache.spark.sql.Dataset[(Int, String)] = [_1: int, _2: string]
joined: org.apache.spark.sql.DataFrame = [idx: int, birth_date: string, gender: string, age: int, u_id: string]

root
 |-- idx: integer (nullable = true)
 |-- birth_date: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- u_id: string (nullable = true)


# joinWith... not join
==========================================
val joinedWith =
  cuUsersWithAge.as("DS_A")
  .joinWith(targetUsers.as("DS_B"), $"DS_A.idx" === $"DS_B.idx")

joinedWith.show()
joinedWith.printSchema
==========================================
joinedWith: org.apache.spark.sql.Dataset[((Int, String, String, Int), (Int, String))] = [_1: struct<_1:int,_2:string,_3:string,_4:int>, _2: struct<_1:int,_2:string>]

root
 |-- _1: struct (nullable = false)
 |    |-- idx: integer (nullable = true)
 |    |-- birth_date: string (nullable = true)
 |    |-- gender: string (nullable = true)
 |    |-- age: integer (nullable = true)
 |-- _2: struct (nullable = false)
 |    |-- idx: integer (nullable = true)
 |    |-- id: string (nullable = true)
 */