-- Databricks notebook source
-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
-- MAGIC 
-- MAGIC val spark: SparkSession = SparkSession.builder().appName("MyApp").getOrCreate()
-- MAGIC 
-- MAGIC def readCSV(directoryPaths: List[String]): Unit = {
-- MAGIC   for (directoryPath <- directoryPaths) {
-- MAGIC     val df: DataFrame = spark.read.format("csv")
-- MAGIC       .option("header", "true")
-- MAGIC       .option("inferSchema", "true")
-- MAGIC       .load(directoryPath)
-- MAGIC     val tableName: String = directoryPath.split("/").last.split("\\.").head
-- MAGIC     df.write.format("delta")
-- MAGIC       .mode(SaveMode.Overwrite)
-- MAGIC       .save(s"/delta/soroush/$tableName")
-- MAGIC   }
-- MAGIC }
-- MAGIC 
-- MAGIC val fileList: List[String] = List("/FileStore/tables/dictionary.csv", "/FileStore/tables/summerNew.csv", "/FileStore/tables/winter_newSeason.csv")
-- MAGIC readCSV(fileList)

-- COMMAND ----------

DROP TABLE IF EXISTS countriessama;
CREATE TABLE countriessama USING DELTA LOCATION '/delta/soroush/dictionary';

DROP TABLE IF EXISTS Summersama;
CREATE TABLE Summersama USING DELTA LOCATION '/delta/soroush/summerNew';

DROP TABLE IF EXISTS Wintersama;
CREATE TABLE Wintersama USING DELTA LOCATION '/delta/soroush/winter_newSeason';

-- COMMAND ----------

--DROP TABLE IF EXISTS Country;
--CREATE TABLE Country
--AS
--SELECT Code, Population
--FROM countriesALL

-- COMMAND ----------

DROP TABLE IF EXISTS Countrysama;
CREATE TABLE Countrysama (
  Id INT,
  Code VARCHAR(3),
  GDP DECIMAL(10,2),
  Name VARCHAR(50), 
  Population BIGINT
)
USING DELTA;

INSERT INTO Countrysama (Id, Code, GDP,Name, Population)
SELECT monotonically_increasing_id()+1, Code, GDP,Country, Population
FROM countriessama;

update Countrysama
set Name = REPLACE(Name,'*','')
where Name like '%*';

SELECT * FROM countrysama

-- COMMAND ----------

DROP TABLE IF EXISTS Totalsama;
CREATE TABLE Totalsama (
    Year int  ,
    City varchar(255),
    Sport varchar(255),
    Discipline varchar(255),
    Athlete varchar(255),
    Country varchar(255),
    Gender varchar(255),
    Event varchar(255),
    Medal varchar(255),
    Season varchar(255)
 );
 
INSERT INTO Totalsama
SELECT *
FROM Wintersama
UNION
SELECT *
FROM Summersama;

SELECT * FROM totalsama

-- COMMAND ----------

DROP TABLE IF EXISTS Medalsama;
CREATE TABLE Medalsama (
  Id INT,
  Name VARCHAR(50)
)
USING DELTA;

INSERT INTO Medalsama (Id, Name)
SELECT monotonically_increasing_id()+1, Medal
FROM Totalsama
Group by Medal;

SELECT * FROM Medalsama

-- COMMAND ----------

DROP TABLE IF EXISTS Gendersama;
CREATE TABLE Gendersama (
  Id INT,
  Name VARCHAR(50)
)
USING DELTA;

INSERT INTO Gendersama (Id, Name)
SELECT monotonically_increasing_id()+1, Gender
FROM Totalsama
Group by Gender;

SELECT * FROM Gendersama

-- COMMAND ----------

DROP TABLE IF EXISTS Seasonsama;
CREATE TABLE Seasonsama (
  Id INT,
  Name VARCHAR(50)
)
USING DELTA;

INSERT INTO Seasonsama (Id, Name)
SELECT monotonically_increasing_id()+1, Season
FROM Totalsama
Group by Season;

SELECT * FROM Seasonsama

-- COMMAND ----------

DROP TABLE IF EXISTS Sportsama;
CREATE TABLE Sportsama (
  Id INT,
  Name VARCHAR(50)
)
USING DELTA;

INSERT INTO Sportsama (Id, Name)
SELECT monotonically_increasing_id()+1, Sport
FROM Totalsama
Group by Sport;

SELECT * FROM Sportsama

-- COMMAND ----------



-- COMMAND ----------

DROP TABLE IF EXISTS Citysama;
CREATE TABLE Citysama (
  Id INT,
  Name VARCHAR(50)
)
USING DELTA;

INSERT INTO Citysama (Id, Name)
SELECT monotonically_increasing_id()+1, City
FROM Totalsama
Group by City;

SELECT * FROM Citysama

-- COMMAND ----------

DROP TABLE IF EXISTS Disciplinesama;
CREATE TABLE Disciplinesama (
    ID INT, 
    SportID INT,
    Name VARCHAR(255)
);

INSERT INTO Disciplinesama (ID, SportID, Name)
SELECT monotonically_increasing_id()+1, s.Id, t.Discipline
FROM Sportsama s
JOIN Totalsama t ON s.Name = t.Sport
GROUP BY s.Id, t.Discipline;

SELECT * FROM disciplinesama

-- COMMAND ----------

DROP TABLE IF EXISTS Eventsama;
CREATE TABLE Eventsama (
    ID INT, 
    DisciplineID INT,
    Name VARCHAR(255)
);

INSERT INTO Eventsama (ID, DisciplineID, Name)
SELECT monotonically_increasing_id()+1, d.Id, t.Event
FROM Discipline d
JOIN Totalsama t ON d.Name = t.Discipline
GROUP BY d.Id, t.Event;

SELECT * FROM Eventsama

-- COMMAND ----------

DROP TABLE IF EXISTS Hostsama;

CREATE TABLE Hostsama (
    ID INT, 
    CityID INT,
    SeasonID INT,
    Year INT
);

INSERT INTO Hostsama (ID, CityID, SeasonID, Year)
SELECT monotonically_increasing_id()+1, c.Id, s.Id, t.Year
FROM Totalsama t
JOIN Citysama c ON t.City = c.Name
JOIN Seasonsama s ON t.Season = s.Name
GROUP BY c.Id, s.Id, t.Year;

SELECT * FROM Hostsama;


-- COMMAND ----------

DROP TABLE IF EXISTS Athletesama;

CREATE TABLE Athletesama (
    ID INT, 
    CountryID INT,
    GenderID INT,
    Name VARCHAR(255)
);

INSERT INTO Athletesama (ID, CountryID, GenderID, Name)
SELECT monotonically_increasing_id()+1, c.Id, g.Id, t.Athlete
FROM Totalsama t
JOIN Countrysama c ON t.country = c.Code
JOIN Gendersama g ON t.gender = g.Name
GROUP BY c.Id, g.Id, t.Athlete;

SELECT * FROM Athletesama;



-- COMMAND ----------

DROP TABLE IF EXISTS Resultsama;

CREATE TABLE Resultsama (
    ID INT, 
    EventID INT,
    MedalID INT,
    HostID INT,
    AthleteID INT
);

INSERT INTO Resultsama (ID, EventID, MedalID, HostID, AthleteID)
SELECT monotonically_increasing_id()+1, e.Id, m.Id, h.Id, a.ID
FROM Totalsama t
JOIN Eventsama e ON t.event = e.Name
JOIN Medalsama m ON t.medal = m.Name
JOIN Hostsama h ON t.year = h.year
JOIN Athletesama a ON t.athlete = a.Name
GROUP BY e.Id, m.Id, h.Id, a.ID;

SELECT * FROM Resultsama;
SELECT Count(*) FROM Resultsama;


-- COMMAND ----------


(SELECT m.Id FROM Totalsama t Right JOIN Medalsama m ON t.medal = m.Name)
   
