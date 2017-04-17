

# Set the system environment variables
Sys.setenv(SPARK_HOME = "G:/Spark/spark-2.0.2-bin-hadoop2.7")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

#load the Sparkr library
library(SparkR)
library(plotly)
library(forecast)
library(rpart)
library(party)

# Create a spark context and a SQL context
sc <- sparkR.init(master = "local")
sqlContext <- sparkRSQL.init(sc)

#create a sparkR DataFrame
DF <- createDataFrame(sqlContext, faithful)
head(DF)


# Create a simple local data.frame
localDF <- data.frame(SourceName=c("Nacre", "POOL3G"), StatsKey=c("COUNT_NULL_OR_BLANK", "COUNT_ALL_ROWS"), StatsValue=c(400,0))

# Convert local data frame to a SparkR DataFrame
df <- createDataFrame(sqlContext, localDF)

# Print its schema
printSchema(df)
# root
#  |-- name: string (nullable = true)
#  |-- age: double (nullable = true)

# Create a DataFrame from a JSON file
path <- file.path(Sys.getenv("SPARK_HOME"), "examples/src/main/resources/NonTradeStatsNew.json")
statsDF <- jsonFile(sqlContext, path)
printSchema(statsDF)


# Register this DataFrame as a table.
registerTempTable(statsDF, "DAMSTATS")

# SQL statements can be run by using the sql methods provided by sqlContext
statisticswithNullRows <- sql(sqlContext, "SELECT SourceName,DateTime, Avg(StatsValue) AvgNullRowsStats FROM DAMSTATS WHERE StatsKey = 'COUNT_NULL_OR_BLANK' group by SourceName, DateTime ORDER BY SourceName")
statisticswitAllRows <- sql(sqlContext, "SELECT SourceName, DateTime, Avg(StatsValue) AvgAllRowsStats FROM DAMSTATS WHERE StatsKey = 'COUNT_ALL_ROWS' group by SourceName, DateTime ORDER BY SourceName ")


print(statisticswithNullRows)

statisticsCount <- sql(sqlContext, "SELECT count(*) FROM DAMSTATS")

# Call collect to get a local data.frame
statisticsWithNullRowsLocalDF <- collect(statisticswithNullRows)
statisticsWithAllRowsLocalDF <- collect(statisticswitAllRows)
statisticsCountLocalDF <- collect(statisticsCount)


# Print the teenagers in our dataset 
print(statisticsWithNullRowsLocalDF)
print(statisticsWithAllRowsLocalDF)
print( statisticsCountLocalDF)


SourceNameWithNullVaules <- c(statisticsWithNullRowsLocalDF$SourceName)
StatsTimeWithNullValues <-  c(statisticsWithNullRowsLocalDF$DateTime)
StatsValueWithNullValues <- c(statisticsWithNullRowsLocalDF$AvgNullRowsStats)


SourceNameWithAllRows <- c(statisticsWithAllRowsLocalDF$SourceName)
StatsTimeWithAllRows <-  c(statisticsWithAllRowsLocalDF$DateTime)
StatsValueWithAllRows <- c(statisticsWithAllRowsLocalDF$AvgAllRowsStats)



print(StatsValueWithAllRows)
result.median <- median(StatsValueWithAllRows)
print(result.mean)




fitAllRows <- ets(StatsValueWithAllRows)
foreCastAllRows <- forecast(fitAllRows, h = 48, level = c(80, 95))
print(foreCastAllRows)


fitNullRows <- ets(StatsValueWithNullValues)
foreCastWithNullRows <- forecast(fitNullRows, h = 48, level = c(80, 95))
print(foreCastWithNullRows)



#plot.ts(foreCastWithNullRows$fitted)
#plot.ts(foreCastAllRows$fitted)

plot(StatsValueWithNullValues, StatsTimeWithNullValues, col="Blue")

y<- StatsValueWithNullValues
x<- StatsTimeWithNullValues

sensor <- ts(StatsValueWithNullValues, frequency=12)
fit<- auto.arima(sensor)
fcast<- forecast(fit)
plot(fcast)
#LH.pred<-predict(fit,n.ahead=24)
#plot(sensor,ylim=c(0,10),xlim=c(0,5),type="o", lwd="1")
#lines(LH.pred$pred,col="red",type="o",lwd="1")
grid()


#plot(predict(foreCastAllRows, n.ahead=20), lty = c(1,3), col=c(5,2))
#plot(predict(foreCastWithNullRows, n.ahead=20), lty = c(1,3), col=c(5,2))
#plot(predict(StatsValueWithNullValues))


# Stop the SparkContext now
sparkR.stop()


