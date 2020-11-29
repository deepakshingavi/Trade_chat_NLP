
import com.ds.practise.nlp.common.AppProperties
import com.ds.practise.nlp.analyzer.TradeChatAnalyzer
import com.ds.practise.nlp.model.Email
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SQLImplicits, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite


class TradeChatAnalyzerTest extends AnyFunSuite
  with BeforeAndAfter {
  self =>

  var spark: SparkSession = _
  var baseDf: DataFrame = _
  var analyzer: TradeChatAnalyzer = _
  var props: AppProperties = _

  private object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  before {
    spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
    val files = Seq("src/test/resources/test-config.properties")
    props = new AppProperties(files)

    analyzer = new TradeChatAnalyzer(props)

    baseDf = analyzer.loadData(spark)


  }

  after {
    spark.close()
  }

  test("load text data and verify columns") {
    assertResult(3)(baseDf.count())
    assertResult(Array("domain", "author", "title", "selftext", "created_utc"))(baseDf.columns)
  }

  /*test("output columns must match the expected format") {
    val outputColumns = props.get(Constant.OUTPUT_COLUMNS).split(",").map(col)
    val resultDf = analyzer.formatOutputColumns(baseDf,outputColumns)
    import testImplicits._
    val emails : util.List[Email] = resultDf.as[Email](encoder).collectAsList()
    assertResult(6)(emails.size())
  }*/

  test("chats should be converted to lower case") {
    import testImplicits._
    val sampleDf = spark.read.json("src/test/resources/sample-data/test_1.json")
    val resultDf = analyzer.chatTextToLowerCase(sampleDf)
    val resultTexts = resultDf.select("selftext").as[String].collect()
    val expected = Array("  ![img](untvykuxea151)  &amp;#x200b;  good luck. dr. retard tqqq burry out.", "good saturday morning to all of you here on r/wallstreetbets. i hope everyone on this sub made out pretty nicely in the.", "trading week?  *****  i hope you all have a fantastic weekend and a great trading week ahead r/wallstreetbets!")
    assertResult(expected)(resultTexts)
  }

  test("records should be filtered on minimum words in chats ") {
    import testImplicits._
    val r = new scala.util.Random(31)

    val inputDf = List(
      (1 to 51).map(_ => r.nextString(5)),
      (1 to 10).map(_ => r.nextString(5)),
      (1 to 49).map(_ => r.nextString(5))
    ).toDF("tokens")

    val df = analyzer.filterByMinNoOfWord(inputDf)
    assertResult(1)(df.count())
  }

  test("get records with most no. of words with unique receivers") {
    import testImplicits._
    val r = new scala.util.Random(31)
    val inputDf = List(
      ("ABC@g.com", 76),
      ("XYZ@g.com", 50),
      ("ABC@g.com", 64),
      ("FFF@g.com", 42),
      ("KKK@g.com", 93),
      ("SSS@g.com", 12)
    ).toDF("from", "no_of_words")

    val resultDf = analyzer.getLargeContentForEachAuthor(inputDf)
    val totalRecords = resultDf.count()
    val uniqueAuthors = resultDf.select(countDistinct("from")).as[Long].collect()(0)
    assertResult(totalRecords)(uniqueAuthors)

    val noOfWords = resultDf.select("no_of_words").where($"from" === "ABC@g.com").as[Long].collect()(0)

    assertResult(76)(noOfWords)
  }

  test("fetch matching records with keywords from config") {
    import testImplicits._

    val inputDf = List(
      Array("run", "fun", "trade"),
      Array("ohhhh", "bosss", "hedge", "now", "gas"),
      Array("dividend"),
      Array("buy", "sell", "up", "down")
    ).toDF("filteredTokens")

    val df = analyzer.keywordSearch(inputDf)
    assertResult(3)(df.count())
  }

  test("end to end test to email") {
    val emails = analyzer.processTradeChats(spark)
    val emailsWithoutBOdy = emails.map(email => Email(email.messageId, email.from, email.subject, email.date, ""))
    val expected = Array(
      Email("1572706734@self.wallstreetbets", "bigbear0083@self.wallstreetbets", "Wall Street Week Ahead for the trading week beginning November 4th, 2019", "Sat, 2 Nov 2019 20:28:54 +05:30", ""))
    assertResult(expected)(emailsWithoutBOdy)
  }

  test("all stop words should be removed") {
    import testImplicits._

    val inputDf = List(
      Array("will", "fun", "trade"),
      Array("i", "ve", "hedge", "now", "gas"),
      Array("reddit"),
      Array("buy", "sell", "www", "google", "com")
    ).toDF("tokens")
    val resultDf = analyzer.removeStopWords(inputDf)
    val result = resultDf.as[(Array[String], Array[String], Long)].collect()
    val formattedResult = result.map(row => (row._1.mkString(","), row._2.mkString(","), row._3))

    val expected = Array(
      ("will,fun,trade", "fun,trade", 0),
      ("i,ve,hedge,now,gas", "hedge,gas", 1),
      ("reddit", "", 2),
      ("buy,sell,www,google,com", "buy,sell,google", 3)
    )

    assertResult(expected)(formattedResult)
  }

  test("split sentence into tokens") {
    import testImplicits._
    val inputDf = List(
      "trading week?  *****  I hope you all have a fantastic weekend and a great trading week ahead",
      "I hope everyone on this sub made out pretty nicely in the."
    ).toDF("selftext")

    val formattedResult = analyzer.tokenizeInputText(inputDf).select("tokens").as[Array[String]].collect()
    val expected: Array[Array[String]] = Array(
      Array("trading", "week", "i", "hope", "you", "all", "have", "a", "fantastic", "weekend", "and", "a", "great", "trading", "week", "ahead"),
      Array("i", "hope", "everyone", "on", "this", "sub", "made", "out", "pretty", "nicely", "in", "the")
    )
    assertResult(expected)(formattedResult)
  }

  test("email formatting"){

    val email = Email("messageId", "email@from.com", "subject line", "Wed, 27 May 2020 12:52:08 +00:00", "I understand that most of this sub has the critical reading skills of a 6 year old and the attention big sigma moves happen all the time in of buying aapl or msft for hours, i knew i could immediately buy them both with tqqq and be rewarded": String)

    val formattedEmailText = analyzer.formatEmail(email)

    val expectedText = "Message-ID: messageId\n\rFrom: email@from.com\n\rTo: user@behavox.com\n\rSubject: subject line\n\rDate: Wed, 27 May 2020 12:52:08 +00:00\n\rMime-Version: 1.0\n\rContent-Transfer-Encoding: 7bit\n\rContent-Type: text/plain; charset=utf-8\n\rI understand that most of this sub has the critical reading skills of a 6 year old and the attention\n\rbig sigma moves happen all the time in of buying aapl or msft for hours, i knew i could immediately\n\rbuy them both with tqqq and be rewarded\n\r--------------------------------------\n\r"

    assertResult(expectedText)(formattedEmailText)
  }


}
