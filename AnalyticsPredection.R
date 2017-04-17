

# Set the system environment variables
Sys.setenv(SPARK_HOME = "G:/Spark/spark-2.0.2-bin-hadoop2.7")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

#load the Sparkr library
library(SparkR)
library(plotly)
library(forecast)

# Create a spark context and a SQL context
sc <- sparkR.init(master = "local")
sqlContext <- sparkRSQL.init(sc)

#create a sparkR DataFrame
DF <- createDataFrame(sqlContext, faithful)
head(DF)



# Create a DataFrame from a JSON file
path <- file.path(Sys.getenv("SPARK_HOME"), "examples/src/main/resources/NonTradeStats.json")
statsDF <- jsonFile(sqlContext, path)
printSchema(statsDF)


# Register this DataFrame as a table.
registerTempTable(statsDF, "DAMSTATS")

# SQL statements can be run by using the sql methods provided by sqlContext
statistics <- sql(sqlContext, "SELECT SourceName, StatsKey, StatsValue FROM DAMSTATS")



statisticsCount <- sql(sqlContext, "SELECT count(*) FROM DAMSTATS")

# Call collect to get a local data.frame
statisticsLocalDF <- collect(statistics)
statisticsCountLocalDF <- collect(statisticsCount)


# Print the teenagers in our dataset 
print(statisticsLocalDF)

print( statisticsCountLocalDF)


#plot(statisticsLocalDF[, -c(1,2)])

#with(statisticsLocalDF, plot(StatsKey, count))



SourceName <- c(statisticsLocalDF$SourceName)
StatsKey <-  c(statisticsLocalDF$StatsKey)
StatsValue <- c(statisticsLocalDF$StatsValue)


data <- data.frame(StatsKey, StatsValue, SourceName)

#plot_ly(data, x = ~StatsKey, y = ~StatsValue, type = 'bar', text = SourceName,
#            marker = list(color = 'rgb(158,202,225)',

#                       line = list(color = 'rgb(8,48,107)',
#                                       width = 1.5))) %>%
# layout(title = "Statistics Variation",
#       xaxis = list(title = ""),
#      yaxis = list(title = ""), barmode='group') 




fit <- ets(StatsValue)
fore <- forecast(fit, h = 48, level = c(80, 95))

print(fore)


plot_ly() %>%
  add_lines(x = StatsValue, y = StatsValue, color = I("black"), name = "observed") %>%
  add_lines(x = time(fore$mean), y = fore$mean, color = I("blue"), name = "prediction")


#print(fore)


counts <- table(statisticsCountLocalDF)





# Stop the SparkContext now
sparkR.stop()


