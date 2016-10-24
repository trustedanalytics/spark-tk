# To run this program, run this command:
# R -f arimax.R

# CONSTANTS!
# Set these variables before running
path_to_data_dir="/home/hadoop/spark-tk/regression-tests/generatedata/"
filename="../datasets/tute1.csv"
goldfile="../datasets/arimaxoutput.txt"
#path_to_data_dir=DIRNAME

require(Metrics)
# Only change these if you really need to...
load_data <- function(path_to_data_dir){

    require(lubridate)

    ## Define the analysis dat from which the forecats need to be generated

    # Load data set with weather information...
    data <- read.table(filename, header=T, sep=",")
    colnames(data) <- c("Int", "Date", "Sales", "AdBudget", "GDP")
    # Get training data based on offset from analysis data...

    train <- subset(data, data$Int <= 90)

    # Get test data based on offset from analysis data...
    test <- subset(data, data$Int > 90)

    return(
        list(
            "TRAIN"=train,
            "TEST"=test,
            "TRAIN.xreg"=subset(train, select=c(AdBudget, GDP)),
            "TRAIN.y"=train$Sales,
            "TEST.xreg"=subset(test, select=c(AdBudget, GDP)),
            "TEST.y"=test$Sales
        )
    )
}


require(forecast)

# Get data...
tmp <- load_data(path_to_data_dir)
# Fit with specified parameters...
# ARX
fitted_model_arx <- arima(x=tmp$TRAIN.y, order=c(1,0,0), xreg=tmp$TRAIN.xreg)
predict_arx <- predict(fitted_model_arx, n.ahead=10, newxreg=tmp$TEST.xreg)
# ARIMA
fitted_model_arima <- arima(x=tmp$TRAIN.y, order=c(1,0,1))
predict_arima <- predict(fitted_model_arima, n.ahead=10)
#ARIMAX
fitted_model_arimax <- arima(x=tmp$TRAIN.y, order=c(1,0,1), xreg=tmp$TRAIN.xreg)
predict_arimax <- predict(fitted_model_arimax, n.ahead=10, newxreg=tmp$TEST.xreg)
#MAX
fitted_model_max <- arima(x=tmp$TRAIN.y, order=c(0,0,1), xreg=tmp$TRAIN.xreg)
predict_max <- predict(fitted_model_max, n.ahead=10, newxreg=tmp$TEST.xreg)

sink(goldfile)

print("BEGIN ARIMA PREDICT")
print(array(predict_arima$pred))
print("END ARIMA PREDICT")

print("BEGIN ARX PREDICT")
print(array(predict_arx$pred))
print("END ARX PREDICT")

print("BEGIN ARIMAX PREDICT")
print(array(predict_arimax$pred))
print("END ARIMAX PREDICT")

print("BEGIN MAX PREDICT")
print(array(predict_max$pred))
print("END MAX PREDICT")

sink()
