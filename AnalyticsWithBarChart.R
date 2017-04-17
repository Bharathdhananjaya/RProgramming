

# Set the system environment variables
Sys.setenv(SPARK_HOME = "G:/Spark/spark-2.0.2-bin-hadoop2.7")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

#load the Sparkr library
library(SparkR)
library(plotly)
library(forecast)
library(rpart)

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
path <- file.path(Sys.getenv("SPARK_HOME"), "examples/src/main/resources/NonTradeStats.json")
statsDF <- jsonFile(sqlContext, path)
printSchema(statsDF)


# Register this DataFrame as a table.
registerTempTable(statsDF, "DAMSTATS")

# SQL statements can be run by using the sql methods provided by sqlContext
statisticswithNullRows <- sql(sqlContext, "SELECT SourceName, StatsKey, Avg(StatsValue) AvgStats FROM DAMSTATS WHERE StatsKey='COUNT_NULL_OR_BLANK' group by SourceName, StatsKey ORDER BY SourceName")
statisticswitAllRows <- sql(sqlContext, "SELECT SourceName, StatsKey, Avg(StatsValue) AvgStats FROM DAMSTATS WHERE StatsKey='COUNT_ALL_ROWS' group by SourceName, StatsKey ORDER BY SourceName ")
completeStats <- sql(sqlContext, "SELECT SourceName, StatsKey, Avg(StatsValue) AvgStats FROM DAMSTATS  group by SourceName, StatsKey ORDER BY SourceName" )







print(statisticswithNullRows)

statisticsCount <- sql(sqlContext, "SELECT count(*) FROM DAMSTATS")

# Call collect to get a local data.frame
statisticsWithNullRowsLocalDF <- collect(statisticswithNullRows)
statisticsWithAllRowsLocalDF <- collect(statisticswitAllRows)
statisticsCountLocalDF <- collect(statisticsCount)
completeStatsLocalDF <- collect(completeStats)




# Print the teenagers in our dataset 
print(statisticsWithNullRowsLocalDF)
print(statisticsWithAllRowsLocalDF)
print(statisticsCountLocalDF)
print(completeStatsLocalDF)

#plot(statisticsLocalDF[, -c(1,2)])

#with(statisticsLocalDF, plot(StatsKey, count))



# SourceName <- c(statisticsLocalDF$SourceName)
# StatsKey <-  c(statisticsLocalDF$StatsKey)
# StatsValue <- c(statisticsLocalDF$AvgStats)


#data <- data.frame(StatsKey, StatsValue, SourceName)

# plot_ly(data, x = ~SourceName, y = ~StatsValue, type = 'bar', text = StatsValue,
#             marker = list(color = 'rgb(158,202,225)',
#                        line = list(color = 'rgb(8,48,107)',
#                                        width = 0.5))) %>%
#  layout(title = "Statistics Variation",
#        xaxis = list(title = "Source Name"),
#         yaxis = list(title = "Stats Error Count"), barmode='group')



# 
# plot_ly(data = iris, x = ~SourceName, y = ~StatsKey
#              ,
#              marker = list(size = 10,
#                            color = 'rgba(255, 182, 193, .9)',
#                            line = list(color = 'rgba(152, 0, 0, .8)',
#                                        width = 2))) %>%
#   layout(title = 'Styled Scatter',
#          yaxis = list(zeroline = FALSE),
#          xaxis = list(zeroline = FALSE))



# plot_ly(data, x = ~SourceName, y = ~StatsValue, type = 'bar', name = 'Stats Indicators',
#              marker = list(color = 'rgb(55, 83, 109)')) %>%
#   add_trace(y = ~StatsValue, name = 'StatsValue', marker = list(color = 'rgb(26, 118, 255)')) %>%
#   layout(title = 'Statistics Analysis',
#          xaxis = list(
#            title = "",
#            tickfont = list(
#              size = 14,
#              color = 'rgb(107, 107, 107)')),
#          yaxis = list(
#            title = 'Total Error Counts',
#            titlefont = list(
#              size = 16,
#              color = 'rgb(107, 107, 107)'),
#            tickfont = list(
#              size = 14,
#              color = 'rgb(107, 107, 107)')),
#          legend = list(x = 0, y = 1, bgcolor = 'rgba(255, 255, 255, 0)', bordercolor = 'rgba(255, 255, 255, 0)'),
#          barmode = 'group', bargap = 0.15, bargroupgap = 0.1)



SourceNameWithNullVaules <- c(statisticsWithNullRowsLocalDF$SourceName)
StatsKeyWithNullValues <-  c(statisticsWithNullRowsLocalDF$StatsKey)
StatsValueWithNullValues <- c(statisticsWithNullRowsLocalDF$AvgStats)


SourceNameWithAllRows <- c(statisticsWithAllRowsLocalDF$SourceName)
StatsKeyWithAllRows <-  c(statisticsWithAllRowsLocalDF$StatsKey)
StatsValueWithAllRows <- c(statisticsWithAllRowsLocalDF$AvgStats)





g_range <- range(0, StatsValueWithNullValues, StatsValueWithAllRows)

# plot(StatsValueWithNullValues, type="o", col="blue", ylim=g_range, 
#      axes=FALSE, ann=FALSE)


completeStatsSourceName <- c(completeStatsLocalDF$SourceName)
completeStatsKeyName <- c(completeStatsLocalDF$StatsKey)
completeStatsAvgValue <- c(completeStatsLocalDF$AvgStats)



StatsValueWithAllRows
StatsValueWithAllRows
histTogether = c(StatsValueWithAllRows, StatsValueWithNullValues)

hist(histTogether,col="lightblue", ylim=c(0,10))




counts <- table(statisticsCountLocalDF)





# Stop the SparkContext now
sparkR.stop()


