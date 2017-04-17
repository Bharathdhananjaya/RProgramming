
Sys.setenv(SPARK_HOME = "G:/Spark/spark-2.0.2-bin-hadoop2.7")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

#load the Sparkr library
library(SparkR)
library(plotly)
library(forecast)
library(rpart)

currency <- c(EUR,Us,PAR,IND,cHN)
values <- c(90,60,85,1,10)

vehicle <- c(currency,values)

# plot(values, type="o", col="blue",  axes=FALSE, ann=FALSE, xlim=c(0,10))
# lines(values, type="o", col="red")
# 
# # Make x axis using Mon-Fri labels
# #axis(1, at=1:5, las=1, lab = c("Mon",  "Tue" , "Wed", "Thu", "Fri"), srt=90)
# 
# axis(1, at=1:5, las=1, lab = currency, srt=90)
# 
# 
# axis(2, las=1, at=1:5, lab = values)
# 
# 
# 
# 
# title(xlab="Currency", col.lab=rgb(0,0.5,0))
# title(ylab="values", col.lab=rgb(0,0.5,0))


dotchart(values,labels=currency,cex=1.7,
         main="Currency Prediction",
         xlab="Currency Codes", pch=25, col="Blue", bg="red")


#plot(currency,values, xlab="Currency Codes", ylab="Currency Values", xlim=c(80,200), ylim=c(55,75), main="Height vs Weight", pch=2, cex.main=1.5, frame.plot=FALSE , col="blue")


# plot(values,currency, ylim=c(0,5), col="blue")
# plot(values,currency, col=ifelse(currency>=3,"red","black"), ylim=c(0,10))

