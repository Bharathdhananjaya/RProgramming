

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






.

#http://www.rdatamining.com/examples/time-series-forecasting

url <- "http://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
download.file(url, "eurofxref-hist.zip")

rates <- read.csv(unz("eurofxref-hist.zip", "eurofxref-hist.csv"), 
                  header = T)
rates[1:2,]

rates$Date <- as.Date(rates$Date, "%Y-%m-%d")
str(rates$Date)

range(rates$Date)
## [1] "1999-01-04" "2014-07-01"
rates <- rates[order(rates$Date), ]
## plot time series
plot(rates$Date, rates$USD,type = "l")


head(rates$Date, 20)

years <- format(rates$Date,"%y")
tab  <- table(years)
tab
mean(tab[1:(length(tab) - 1)])
  
print(rates)


  source("forecast.R")  ## see code file in section 5
  result.arima <- forecastArima(rates, n.ahead = 90)

  source("plotForecastResult.R")  ## see code file in section 5
  plotForecastResult(result.arima, title = "Exchange rate forecasting with ARIMA")

  result.arima


str(rates$Date)


# 
# summary(relation)
# weight
# fitted(relation)
# 
# #plot(weight, height, abline(lm(weight ~ height)),cex = 1.3,pch = 16,xlab = "Weight in Kg",ylab = "Height in cm", col="red")
# 
# predictedValues = data.frame(height, fitted(relation))
# 
# print(predictedValues)
# 
# plot(person$height, cex = 1.3,xlab = "Weight in Kg",ylab = "Height in cm", col="red", ylim=c(0,100))
# lines(predictedValues$fitted.relation., type='o', col='blue')
# 



thuesen<-data.frame(
  blood.glucose=c(15.3, 10.8, 8.1, 19.5, 7.2, 5.3, 9.3,
                  11.1, 7.5, 12.2, 6.7, 5.2, 19.0, 15.1, 6.7, 8.6, 4.2, 10.3,
                  12.5, 16.1, 13.3, 4.9, 8.8, 9.5),
  short.velocity=c(1.76, 1.34, 1.27, 1.47, 1.27, 1.49, 1.31, 1.09,
                   1.18, 1.22, 1.25, 1.19, 1.95, 1.28, 1.52, NA, 1.12, 1.37, 1.19,
                   1.05, 1.32, 1.03, 1.12, 1.70))



height  <-  c(151, 174, 138, 186, 128, 136, 179, 163, 152, 131)
weight <-  c(63, 81, 56, 91, 47, 57, 76, 72, 62, 48)

person <- data.frame(height,weight)

relation <- lm(weight ~ height,person)



library(ISwR)
data(person)










lm.weight <-lm(person$weight ~ person$height, data=person)


person$height
person$weight
fitted(lm.weight)

fitted(lm.weight)

resid(lm.weight)


options(na.action="na.exclude")

# with(person, plot(person$height, person$weight,
#                    xlab="Person Height", ylab="Person Weight",
#                    main="Height VS Weight Prediction"))
# 
# abline(lm.weight)
# with(thuesen, segments(x0=person$height, 
#                        y0=fitted(lm.weight),
#                        x1=person$height, 
#                        y1=person$weight, col = c("blue", "red"), lwd = c(1, 4), 
#                        lty = 1:2))


# require(ggplot2)
# 
# plot(person$weight,person$height,ylim=range(c(y1,y2)),xlim=range(c(x1,x2)), type="l",col="red")
# lines(x2,y2,col="green")#plot(weight,height,col = "blue",main = "Height & Weight Regression",  abline(lm(weight ~ height)),cex = 1.3,pch = 16,xlab = "Weight in Kg",ylab = "Height in cm", col="red" )



# p1a<-ggplot(data=person,aes(x=date,y=observed)) 
# p1a<-p1a+geom_line(col='red')
# p1a<-p1a+geom_line(aes(y=fitted),col='blue')
# p1a<-p1a+geom_line(aes(y=forecast))+geom_ribbon(aes(ymin=lo95,ymax=hi95),alpha=.25)
# p1a<-p1a+scale_x_date(name='',breaks='1 year',minor_breaks='1 month',labels=date_format("%b-%y"),expand=c(0,0))
# p1a<-p1a+scale_y_continuous(name='Units of Y')
# p1a<-p1a+labs(title='Arima Fit to Simulated Data\n (black=forecast, blue=fitted, red=data, shadow=95% conf. interval)')


