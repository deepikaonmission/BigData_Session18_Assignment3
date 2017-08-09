/* <<<<<<<<<<<<<---------------- QUERIES ---------------->>>>>>>>>>>>>>>
1) Considering age groups of < 20 , 20-35, 35 > ,Which age group spends the most amount of money travelling.
2) What is the amount spent by each age-group, every year in travelling?

NOTE: Since DataSets in all three Assignments i.e. 18.1,18.2,18.3 are same, so DataSet inside 18.2 is considered
 */

import org.apache.spark.sql.{Row,Column,SparkSession,SQLContext}  //Explanation is already given in Assignment18.1

object Session18Assign3 extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Session18Assign3")
    .config("spark.sql.warehouse.dir", "file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 18/Assignments/Assignment2/DataSet")
    .getOrCreate()
  //Explanation is already given in Assignment 18.1

  val df1 = spark.sqlContext.read.csv("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 18/Assignments/Assignment2/DataSet/S18_Dataset_Holidays.txt")
  val df2 = spark.sqlContext.read.csv("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 18/Assignments/Assignment2/DataSet/S18_Dataset_Transport.txt")
  val df3 = spark.sqlContext.read.csv("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 18/Assignments/Assignment2/DataSet/S18_Dataset_User_details.txt")

  /*Explanation of above three lines
  -> to work with spark sql, sqlContext is used
  -> here df1,df2,df3 variables with type sql.DataFrame are created by reading csv file from the specified locations
  */


  df1.show() //to show the data inside DataFrames df1,df2,df3 in tabular format, show() method is used
  //REFER Screenshot 1 for output

  df2.show()
  //REFER Screenshot 2 for output

  df3.show()
  //REFER Screenshot 3 for output


  import spark.implicits._ //to work with case class this import is required just before case class
  case class holidaysClass(user_id: Int, src: String, dest: String, travel_mode: String, distance: Int, year_of_travel: Int)

  /* Explanation of above line
  -> case class "holidaysClass" is created, in order to
     * provide descriptive name to columns
     * and to infer schema (i.e. which are numeric columns, string columns etc.)
   */

  val df4 = df1.map {
    case Row(
    a: String,
    b: String,
    c: String,
    d: String,
    e: String,
    f: String) => holidaysClass(user_id = a.toInt, src = b, dest = c, travel_mode = d, distance = e.toInt, year_of_travel = f.toInt)
  }
  /* Explanation of above lines
  -> df4 -->> Dataset[Session18Assign2.holidaysClass] is created
  -> where df1 is mapped with case class
  */

  df4.show()
  //REFER Screenshot 4 for output

  case class transportClass(travel_mode: String, cost_per_unit: Int)

  val df5 = df2.map {
    case Row(
    a: String,
    b: String
    ) => transportClass(travel_mode = a, cost_per_unit = b.toInt)
  }

  df5.show()
  //REFER Screenshot 5 for output

  case class userClass(user_id: Int, name: String, age: Int)

  val df6 = df3.map {
    case Row(
    a: String,
    b: String,
    c: String
    ) => userClass(user_id = a.toInt, name = b, age = c.toInt)
  }

  df6.show()
  //REFER Screenshot 6 for output

  df4.createOrReplaceTempView("holidaysTable")
  df5.createOrReplaceTempView("transportTable")
  df6.createOrReplaceTempView("userTable")

  /* Explanation of above three lines
  -> holidaysTable Temporary View is created from Dataset df4
  -> transportTable Temporary View is created from Dataset df5
  -> userTable Temporary View is created from Dataset df6
  */

  spark.sql("select * from holidaysTable").show() //data is selected using select query from holidaysTable, and displayed in tabular format
  //REFER Screenshot 7 for output

  spark.sql("select * from transportTable").show() //data is selected using select query from transportTable, and displayed in tabular format
  //REFER Screenshot 8 for output

  spark.sql("select * from userTable").show() //data is selected using select query from userTable, and displayed in tabular format
  //REFER Screenshot 9 for output

  //<<<<<<<<----------- Creation of ageGroups -------------->>>>>>>>>>>>>>>

  val df6_withAgeGP =df6.map(Row => {
    if(Row.age<20) (Row.user_id,Row.name,Row.age,"ageGP1 (< 20)")
    else if(Row.age>=20 && Row.age<=35) (Row.user_id,Row.name,Row.age,"ageGP2 (20-35)")
    else (Row.user_id,Row.name,Row.age,"ageGP3 (> 35)")
  })
  //output (e.g.) -->> _1,_2,_3,_4

  /*Explanation
  -> here, ageGroups are created i.e.
     * ageGP1 for age < 20
     * ageGP2 for age between 20-35
     * ageGP3 for age > 35
  -> using, Row where it represents complete rowset
     * df6 is mapped using map
     * where Row is received and Row.user_id,Row.name and Row.age based on different conditions are returned
  -> finally, a new Dataset df6_withAgeGP is created with fields _1,_2,_3,_4
   */

  //df6_withAgeGP.printSchema()      //prints schema
  df6_withAgeGP.show()             //shows result in tabular format
  //REFER Screenshot 10 for output

  df6_withAgeGP.createOrReplaceTempView("userAgeGPTable")
  //to rename fields of above dataset this temporary view is created

  val df6_withAgeGPAlias =spark.sql("select _1 as user_id,_2 as name,_3 as age,_4 as age_group from userAgeGPTable")
  //query provides alias to all fields of above view

  //df6_withAgeGPAlias.printSchema()      //prints schema
  df6_withAgeGPAlias.show()               //shows result in tabular format
  //REFER Screenshot 11 for output


  df6_withAgeGPAlias.createOrReplaceTempView("userAgeGPTableNew")      //temporary view is created

  //spark.sql("select * from userAgeGPTableNew").show()        //result is shown in tabular format



  //<<<<<<<<<------------ QUERY 1 ----------->>>>>>>>>>
  //1. Considering age groups of < 20 , 20-35, 35 > ,Which age group spends the most amount of money travelling.
  val ageGroupSpendMoneyOnTravel = spark.sql("select u.age_group,sum(t.cost_per_unit) as moneySpent from userAgeGPTableNew u" +
    " join holidaysTable h" +
    " join transportTable t" +
    " where u.user_id = h.user_id" +
    " and h.travel_mode = t.travel_mode" +
    " group by u.age_group" +
    " order by u.age_group")
  /*Explanation of above code
  -> Three tables are joined i.e. userAgeGPTableNew, holidaysTable, transportTable
  -> select query fetches age_group and cost of travel by each age_group depending on
     * join condition i.e.
       ** user_id of u matches with user_id of h
       ** and travel_mode of h matches with travel_mode of t
     * and group by clause where grouping is done on the basis of age_group
  -> finally result are sorted in ascending order (default order) based on age_group
  -> at the last ageGroupSpendMoneyOnTravel DataFrame is created
  */

  ageGroupSpendMoneyOnTravel.show()      //shows result in tabular format
  //REFER Screenshot 12 for output

  ageGroupSpendMoneyOnTravel.createOrReplaceTempView("moneySpendOnTravelTable") //creation of temporary view

  spark.sql("select age_group from moneySpendOnTravelTable" +
    " where moneySpent IN (select max(moneySpent) from moneySpendOnTravelTable)").show()
  /* Explanation of above query
  -> select query fetches age_group from moneySpendOnTravelTable
  -> depending on condition where moneySpent matches only max(moneySpent)
  -> finally results are shown in tabular format
  */
  //REFER Screenshot 13 for output



  //<<<<<<<<<------------ QUERY 2 ----------->>>>>>>>>>
  //2. What is the amount spent by each age-group, every year in travelling?
  spark.sql("select h.year_of_travel as year,u.age_group,sum(t.cost_per_unit) as moneySpent from holidaysTable h" +
    " join userAgeGPTableNew u" +
    " join transportTable t" +
    " where h.user_id = u.user_id" +
    " and h.travel_mode = t.travel_mode" +
    " group by h.year_of_travel,u.age_group" +
    " order by h.year_of_travel,u.age_group").show()
  /*Explantion of above query
  -> Three tables are joined i.e. userAgeGPTableNew, holidaysTable, transportTable
  -> select query fetches year, age_group, sum(cost_per_unit) from holidaysTable, userAgeGPTableNew and transportTable depending on
     * join condition where
       ** user_id of h matches with user_id of u
       ** and travel_mode of h matches witht travel_mode of t
     * and group by clause where
       ** grouping is done based on year_of_travel and age_group
  -> result of select query is sorted in ascending order (default order) based on year_of_travel and age_group
  -> finally results are shown in tabular format
  */
  //REFER Screenshot 14 for output
}