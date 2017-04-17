

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

AvgError  <-  c(151, 174, 138, 186, 128, 136, 179, 163, 152, 131)


StdDev <-  c(5, 10, 0, 8, 6, 3, 30, 40, 62, 48)

relation <- lm(StdDev ~ AvgError)

print(relation)

a <- data.frame((x=20))


result <- predict(relation, a)
print(result)


# plot(AvgError,StdDev,main = "Error Count Prediction")
#  abline(relation, col=2, lwd=3)



values <- c(27, 27, 7, 0, 39, 40, 24, 45, 36, 37, 31, 47, 16, 24, 6, 21, 
            35, 36, 21, 40, 32, 33, 27, 42, 14, 21, 5, 19, 31, 32, 19, 36, 
            29, 29, 24, 42, 15, 0, 21)


train_ts<- ts(values, frequency=12)


fit2<-ets(train_ts, model="ZZZ", damped=TRUE, alpha=NULL, beta=NULL, gamma=NULL, 
          phi=NULL, additive.only=FALSE, lambda=TRUE, 
          lower=c(0.0001,0.0001,0.0001,0.8),upper=c(0.9999,0.9999,0.9999,0.98), 
          opt.crit=c("lik","amse","mse","sigma","mae"), nmse=3, 
          bounds=c("both","usual","admissible"), ic=c("aicc","aic","bic"),
          restrict=TRUE)  
ets <- forecast(fit2,h=20)

print(ets)

plot(ets)

lines(fit2$states[,1],col='red')




#  
#  plot_ly() %>%
#    add_lines(x = AvgError, y = StdDev, color = I("black"), name = "observed") %>%
#    add_lines(x = time(fore$mean), y = fore$mean, color = I("blue"), name = "prediction")
 
 
#plot(fore)

