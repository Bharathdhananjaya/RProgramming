    
  
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
  path <- file.path(Sys.getenv("SPARK_HOME"), "examples/src/main/resources/NonTradeStats.json")
  statsDF <- jsonFile(sqlContext, path)
  printSchema(statsDF)
  
  
  # Register this DataFrame as a table.
  registerTempTable(statsDF, "DAMSTATS")
  
  # SQL statements can be run by using the sql methods provided by sqlContext
  statisticswithNullRows <- sql(sqlContext, "SELECT SourceName, StatsKey, Avg(StatsValue) AvgStats FROM DAMSTATS WHERE StatsKey LIKE 'COUNT_NULL_OR_BLANK%' group by SourceName, StatsKey ORDER BY SourceName")
  statisticswitAllRows <- sql(sqlContext, "SELECT SourceName, StatsKey, Avg(StatsValue) AvgStats FROM DAMSTATS WHERE StatsKey LIKE 'COUNT_ALL_ROWS%' group by SourceName, StatsKey ORDER BY SourceName ")
  
  
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
  
  summary(statisticsCountLocalDF)
  

  df <- read.table(text = '
     Quarter Coupon      Total
                   1   "Dec 06"  25027.072  132450574
                   2   "Dec 07"  76386.820  194154767
                   3   "Dec 08"  79622.147  221571135
                   4   "Dec 09"  74114.416  205880072
                   5   "Dec 10"  70993.058  188666980, header=TRUE)
str(df)
  
  
  Total <- c(132450574, 194154767, 221571135)
  Coupon <- c(25027.072, 76386.820,79622.147)
  
  model <- lm(Total~Coupon, data=df )
new.df <- data.frame(Total=c(79037022, 83100656, 104299800))
print(predict(model, new.df))
  
  
  SourceNameWithNullVaules <- c(statisticsWithNullRowsLocalDF$SourceName)
  StatsKeyWithNullValues <-  c(statisticsWithNullRowsLocalDF$StatsKey)
  StatsValueWithNullValues <- c(statisticsWithNullRowsLocalDF$AvgStats)
  
  
  SourceNameWithAllRows <- c(statisticsWithAllRowsLocalDF$SourceName)
  StatsKeyWithAllRows <-  c(statisticsWithAllRowsLocalDF$StatsKey)
  StatsValueWithAllRows <- c(statisticsWithAllRowsLocalDF$AvgStats)
  
  
  dataFrame <- data.frame(SourceName=SourceNameWithNullVaules, StatsKey= StatsKeyWithNullValues, StatsValue=StatsValueWithNullValues)
  newDf <- createDataFrame(sqlContext, dataFrame)
  
  print(StatsValueWithAllRows)
  result.median <- median(StatsValueWithAllRows)
  print(result.mean)
  
  
  g_range <- range(0, StatsValueWithNullValues, StatsValueWithAllRows)
  

  plot(StatsValueWithNullValues, type='o', col='blue', ylim=g_range, axes=FALSE, ann=FALSE)
  lines(StatsValueWithAllRows, type='o', col='red')
  
  
  fit <- ets(StatsValueWithAllRows)
  fore <- forecast(fit, h = 48, level = c(80, 95))
  

  
  Mat <- data.matrix(dataFrame, rownames.force = NA)
  
  
  
  axis(1, at= 1:5, lab =SourceNameWithAllRows)
  
  axis(2, las=1, at = seq(0, 200, 10))
  box()
  
  lines(StatsValueWithAllRows, type="o", pch=22, lty=2, col="red")
  
  title(main="DAM Stats Analysis", col.main="red", font.main=4)
  
  legend(1, g_range[2], c("AVG_NULL_VALUES","AVG_TOTAL_COUNT"), cex=0.8, col=c("blue","red"), pch=21:22, lty=1:2)
  
  
  title(xlab="Non Trade Source", col.lab=rgb(0,0.5,0))
  title(ylab="Error Counts", col.lab=rgb(0,0.5,0))
  
  

  
  
  
  
  
  counts <- table(statisticsCountLocalDF)
  
  
  # print(head(statsDF))
  # input.dat <- statsDF[(1:105),]
  # # Give the chart file a name.
  # png(file = "decision_tree.png")
  # 
  # # Create the tree.
  # output.tree <- ctree(
  #   SourceName ~  StatsValue, 
  #   data = input.dat)
  # 
  # # Plot the tree.
  # plot(output.tree)
  # 
  # # Save the file.
  # dev.off()
  # 
  
  
  # Stop the SparkContext now
  sparkR.stop()
  
  
