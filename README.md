# nlp-chat-analyzer
Identify chats with trade related topics out of random chats meesage

### Pre-Requisites
* Java 8+
* Scala 2.12+
* Spark 3.0.0
* Maven 3.5.0

#### Project Details
* src/main/scala - Contains Scala source code
* src/test/scala - Contains Scala test source code  
* scripts - Contains support shell script to build and run the project


#### Build
Executes the unit test case and build the jar 
```shell script
./scripts/build.sh
```
at 
`target/kafka-stream-analyzer-1.0.jar`


#### Run
Run the Spark submit command as follows

Program arguments
1. Application configuration path

Main Classes :
* Main  - Triggers Spark batch job to run data pipeline process to ingest chat logs and give out emails
```shell script
./bin/spark-submit --class "com.analytics.entry.ActionCountByHotel" \
  --master local[*] /jar-path/nlp-chat-analyzer-1.0.jar config.properties
```

Note : Please edit and use `default-config.properties` as a template config.properties file for input.

#### Project Details
* BaseChatAnalyzer - Defines the abstract behaviour of ideal chat analyzer should have along with some default implementations.
This class can be extended to change as per input dataset formats.
* TradeChatAnalyzer - Contains concrete implementation to process chat data in the sample logs

#### Sample Data 
* `src/main/resources/sampleData/test.json` - Sample input chats
* `src/main/resources/sampleDataOutput/emails.txt` - Sample output in text
  
#### Assumptions
1. `[deleted]` author are not valid authors so they will be filtered out from the data set.
2. `self.wallstreetbets` is a valid domain and `ABC@self.wallstreetbets` is a valid email id.
3. No actual email pushing is required and hence published all the email to a single text file.
4. A valid trading chat with most no. of words in `selftext` by an author should be selected to email.
e.g. If there are two trading chats for the same with no. of words `selftext` as 51 and 500 then the chat which has no .of words as 500 will 
be selected in the result.
5. Token Stemming is not required here as it has known issues like it stems `trading` to `trad`.    
