----CLEANING STOPWORDS FROM ALL THE BOOKS
--val text = sc.textFile("hdfs://armaster:9000/hadoop-user/data/AMidSummerNightDream.txt").map(line=>line.toLowerCase).map(line=>line.split("\\W+")).toDF("col1")

--val stopWords = sc.textFile("hdfs://armaster:9000/hadoop-user/data/stopWords").map(line=>line.split("\\W+")).toDF("col2")

--val result = wordsWithStopWords.except(stopWords)

val booksrdd = sc.textFile("file:////home/elham/Documents/hwsolutions/books/*").map{line=>line.toLowerCase().split("\\W+").filter(!stopWords.contains(_)).filter(_.length>0)}.map(_.mkString(" "))


drop table if exists BOOK;

--CREATING TABLE bookNGRAMS
CREATE TABLE IF NOT EXISTS BOOK(
line string)
PARTITIONED BY(book_name string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

--LOADING DATA INTO THE TABLE BOOKNGRAMS
LOAD DATA LOCAL INPATH '/home/administrator/Documents/Data/books/A mid summer night dream.txt' OVERWRITE INTO TABLE BOOK
Partition(book_name ='A mid summer night dream');

LOAD DATA LOCAL INPATH '/home/administrator/Documents/Data/books/Hamlet.txt' OVERWRITE INTO TABLE BOOK
Partition(book_name ='Hamlet');

LOAD DATA LOCAL INPATH '/home/administrator/Documents/Data/books/King Richard III.txt' OVERWRITE INTO TABLE BOOK
Partition(book_name ='King Richard III');

LOAD DATA LOCAL INPATH '/home/administrator/Documents/Data/books/MacBeth.txt' OVERWRITE INTO TABLE BOOK
Partition(book_name ='MacBeth');

LOAD DATA LOCAL INPATH '/home/administrator/Documents/Data/books/Othello.txt' OVERWRITE INTO TABLE BOOK
Partition(book_name ='Othello');

LOAD DATA LOCAL INPATH '/home/administrator/Documents/Data/books/Romeo and Juliet.txt' OVERWRITE INTO TABLE BOOK
Partition(book_name ='Romeo and Juliet');

LOAD DATA LOCAL INPATH '/home/administrator/Documents/Data/books/The Merchant of Venice.txt' OVERWRITE INTO TABLE BOOK
Partition(book_name ='The Merchant of Venice');

LOAD DATA LOCAL INPATH '/home/administrator/Documents/Data/books/The tempest.txt' OVERWRITE INTO TABLE BOOK
Partition(book_name ='The tempest');

LOAD DATA LOCAL INPATH '/home/administrator/Documents/Data/books/The tragedy of Julius Casear.txt' OVERWRITE INTO TABLE BOOK
Partition(book_name ='The tragedy of Julius Casear');

LOAD DATA LOCAL INPATH '/home/administrator/Documents/Data/books/The tragedy of King Lear.txt' OVERWRITE INTO TABLE BOOK
Partition(book_name ='The tragedy of King Lear');


----NGRAMS FUNCTION

SELECT EXPLODE(NGRAMS(SENTENCES(LOWER(LINE)), 3, 10)) AS TRIGRAMS FROM BOOK;







	

