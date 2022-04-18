          #####   Initial setup   #####
# Checks current working directory
getwd()

# Sets working directory
# Change the path to where your files are located
setwd("C:\\Users\\brany\\Documents\\Revature\\Training\\Projects\\P2\\Data_setP2")



          #####   The files we are cleaning/changing    #####
# Covid-19 data file
covid_19_data = read.csv("covid_19_data.csv", header = F)

# Times Series files
ts_c19_confirmed = read.csv("time_series_covid_19_confirmed.csv", header = F)
ts_c19_confirmed_us = read.csv("time_series_covid_19_confirmed_US.csv", header = F)
ts_c19_deaths = read.csv("time_series_covid_19_deaths.csv", header = F)
ts_c19_deaths_us = read.csv("time_series_covid_19_deaths_US.csv", header = F)
ts_c19_recovered = read.csv("time_series_covid_19_recovered.csv", header = F)



          #####   Functions   #####
# Convert date to format that is compatible with Hive (ONLY FOR TIMES SERIES)
# Potential formats: M/D/YY, MM/D/YY, M/DD/YY, MM/DD/YY
ts_oldDate_to_newDate <- function(oldDate) {
  tempDate <- strsplit(oldDate, split = "/")
  if (nchar(oldDate) == 6) {
    newDate <- paste("20", tempDate[[1]][[3]], "-0", tempDate[[1]][[1]], "-0", tempDate[[1]][[2]], sep = "")
    }
  else if (nchar(oldDate) == 7) {
      if (nchar(tempDate[[1]][[1]]) == 1) {
        newDate <- paste("20", tempDate[[1]][[3]], "-0", tempDate[[1]][[1]], "-", tempDate[[1]][[2]], sep = "")
      }
      else if (nchar(tempDate[[1]][[1]]) == 2){
        newDate <- paste("20", tempDate[[1]][[3]], "-", tempDate[[1]][[1]], "-0", tempDate[[1]][[2]], sep = "")
      }
    }
  else if (nchar(oldDate) == 8) {
    newDate <- paste("20", tempDate[[1]][[3]], "-", tempDate[[1]][[1]], "-", tempDate[[1]][[2]], sep = "")
  }
  else {
    return(oldDate)
  }
  return(newDate)
}

# same as above, but works for covid_19_data dates format MM/DD/YYYY
oldDate_to_newDate <- function(oldDate) {
  if (nchar(oldDate) == 10){
    tempDate <- strsplit(oldDate, split = "/")
    newDate <- paste(tempDate[[1]][[3]], "-", tempDate[[1]][[1]], "-", tempDate[[1]][[2]], sep = "")
    return(newDate)
  }
  else{
    return(oldDate)
  }
}



          #####   Cleaning Data   #####
## Create csv that is a transpose of time series csv. One column is date, the rest are the countries
# Add up all Provinces/Regions so there is only 1 country.


# ts_19_confirmed
  # creates a transpose of data
transpose1 = t(ts_c19_confirmed)
  # removes columns, Province/State, Lat, Long
remCol1 = transpose1[-c(1,3,4),]
  # fixes row numbers after deleting rows
row.names(remCol1) <- NULL
  # creates a list of new dates from old dates
newDateList = lapply(remCol1[1:nrow(remCol1)], ts_oldDate_to_newDate)

  # All numbers are stored as characters, so we need to convert to numeric
df_clean = remCol1[,-1]
colnames(df_clean) <- NULL
  # now we have a transposed table with country then the rest being the data
transpose2 = data.frame(t(df_clean))
  
  # Fixing South Korea because it's formatted like: Korea, South
  # This causes problems later
  # Following statement gives us the row number and we already know the column
rowNum = which(apply(transpose2, 1, function(x) any(x %in% "Korea, South")))
transpose2[rowNum,1] = "Korea"

  # the columns we want to modify
i = c(2:ncol(transpose2))
  # converts the columns to numeric
transpose2[, i] = apply(transpose2[ , i], 2, function(x) as.numeric(as.character(x)))

  # convert into data.table
#install.packages("data.table", dependencies=TRUE)
library(data.table)
df_table = data.table(transpose2)
  # sum up values by country
df_sum = df_table[, lapply(.SD, sum), by=X1]

  # Set up the two data frames and combine
finalCountries = data.frame(t(df_sum))
finalDate = data.frame(t(data.frame(newDateList)))
final = cbind(finalDate, finalCountries)
row.names(final) <- NULL
colnames(final) <- NULL
  # Change Country/Region to Date
final[1,1] = "Date"

write.table(final, "C:\\Users\\brany\\Documents\\Revature\\Training\\Projects\\P2\\Test\\ts_confirmed_byCountry.csv", sep = ",", row.names = FALSE, col.names = FALSE, quote = FALSE)


#ts_c19_deaths
transpose1 = t(ts_c19_deaths)
remCol1 = transpose1[-c(1,3,4),]
row.names(remCol1) <- NULL
newDateList = lapply(remCol1[1:nrow(remCol1)], ts_oldDate_to_newDate)
df_clean = remCol1[,-1]
colnames(df_clean) <- NULL
transpose2 = data.frame(t(df_clean))
rowNum = which(apply(transpose2, 1, function(x) any(x %in% "Korea, South")))
transpose2[rowNum,1] = "Korea"
i = c(2:ncol(transpose2))
transpose2[, i] = apply(transpose2[ , i], 2, function(x) as.numeric(as.character(x)))

library(data.table)
df_table = data.table(transpose2)
df_sum = df_table[, lapply(.SD, sum), by=X1]

finalCountries = data.frame(t(df_sum))
finalDate = data.frame(t(data.frame(newDateList)))

final = cbind(finalDate, finalCountries)
row.names(final) <- NULL
colnames(final) <- NULL
final[1,1] = "Date"

write.table(final, "C:\\Users\\brany\\Documents\\Revature\\Training\\Projects\\P2\\Test\\ts_deaths_byCountry.csv", sep = ",", row.names = FALSE, col.names = FALSE, quote = FALSE)


#ts_c19_recovered
transpose1 = t(ts_c19_recovered)
remCol1 = transpose1[-c(1,3,4),]
row.names(remCol1) <- NULL
newDateList = lapply(remCol1[1:nrow(remCol1)], ts_oldDate_to_newDate)
df_clean = remCol1[,-1]
colnames(df_clean) <- NULL
transpose2 = data.frame(t(df_clean))
rowNum = which(apply(transpose2, 1, function(x) any(x %in% "Korea, South")))
transpose2[rowNum,1] = "Korea"
i = c(2:ncol(transpose2))
transpose2[, i] = apply(transpose2[ , i], 2, function(x) as.numeric(as.character(x)))

library(data.table)
df_table = data.table(transpose2)
df_sum = df_table[, lapply(.SD, sum), by=X1]

finalCountries = data.frame(t(df_sum))
finalDate = data.frame(t(data.frame(newDateList)))

final = cbind(finalDate, finalCountries)
row.names(final) <- NULL
colnames(final) <- NULL
final[1,1] = "Date"

write.table(final, "C:\\Users\\brany\\Documents\\Revature\\Training\\Projects\\P2\\Test\\ts_recovered_byCountry.csv", sep = ",", row.names = FALSE, col.names = FALSE, quote = FALSE)


## Create the same as above, but for US states


# ts_c19_confirmed_us
transpose1 = t(ts_c19_confirmed_us)
remCol1 = transpose1[-c(1:6,8:11),]
row.names(remCol1) <- NULL
newDateList = lapply(remCol1[1:nrow(remCol1)], ts_oldDate_to_newDate)
df_clean = remCol1[,-1]
colnames(df_clean) <- NULL
transpose2 = data.frame(t(df_clean))
i = c(2:ncol(transpose2))
transpose2[, i] = apply(transpose2[ , i], 2, function(x) as.numeric(as.character(x)))

library(data.table)
df_table = data.table(transpose2)
df_sum = df_table[, lapply(.SD, sum), by=X1]

finalCountries = data.frame(t(df_sum))
finalDate = data.frame(t(data.frame(newDateList)))

final = cbind(finalDate, finalCountries)
row.names(final) <- NULL
colnames(final) <- NULL
final[1,1] = "Date"

write.table(final, "C:\\Users\\brany\\Documents\\Revature\\Training\\Projects\\P2\\Test\\ts_confirmed_US_byState.csv", sep = ",", row.names = FALSE, col.names = FALSE, quote = FALSE)


# ts_c19_deaths_us
transpose1 = t(ts_c19_deaths_us)
remCol1 = transpose1[-c(1:6,8:12),]
row.names(remCol1) <- NULL
newDateList = lapply(remCol1[1:nrow(remCol1)], ts_oldDate_to_newDate)
df_clean = remCol1[,-1]
colnames(df_clean) <- NULL
transpose2 = data.frame(t(df_clean))
i = c(2:ncol(transpose2))
transpose2[, i] = apply(transpose2[ , i], 2, function(x) as.numeric(as.character(x)))

library(data.table)
df_table = data.table(transpose2)
df_sum = df_table[, lapply(.SD, sum), by=X1]

finalCountries = data.frame(t(df_sum))
finalDate = data.frame(t(data.frame(newDateList)))

final = cbind(finalDate, finalCountries)
row.names(final) <- NULL
colnames(final) <- NULL
final[1,1] = "Date"

write.table(final, "C:\\Users\\brany\\Documents\\Revature\\Training\\Projects\\P2\\Test\\ts_deaths_US_byState.csv", sep = ",", row.names = FALSE, col.names = FALSE, quote = FALSE)


## Create csv of covid_19_data, change date to Hive Format, remove Last Update, remove decimals for last 3 columns (optional)
temp = t(t(covid_19_data))
  # Create a list of converted dates
newDateList = lapply(temp[1:nrow(temp),2], oldDate_to_newDate)

  # Gets rid of unnecessary decimal by converting to integer
newConfirmedList = lapply(temp[1:nrow(temp),6], as.integer)
  # as.integer doesn't know what to do with "Confirmed" so it gets converted to NA. Adding it back here
newConfirmedList[1] = temp[[1,6]]
  # Do the same for Deaths/Recovered
newDeathsList = lapply(temp[1:nrow(temp),7], as.integer)
newDeathsList[[1]] = temp[[1,7]]
newRecoveredList = lapply(temp[1:nrow(temp),8], as.integer)
newRecoveredList[[1]] = temp[[1,8]]

  # If we store a list as a dataframe, it will appear as multiple columns instead of 1 column
  # ObservationDate, column 2
finalDate = data.frame(t(data.frame(newDateList)))
  # Confirmed, column 6
finalConfirmed = data.frame(t(data.frame(newConfirmedList)))
  # Deaths, column 7
finalDeaths = data.frame(t(data.frame(newDeathsList)))
  # Recovered, column 8
finalRecovered = data.frame(t(data.frame(newRecoveredList)))

  # Store the columns that we didn't alter
finalBeginning = data.frame(temp[,1])
finalMiddle = data.frame(temp[,c(3,4,5)])

  # Bind everything together
final = cbind(finalBeginning, finalDate, finalMiddle, finalConfirmed, finalDeaths, finalRecovered)
#row.names(final) <- NULL
#colnames(final) <- NULL  Don't need these, just to make final row/column headers look nice but we don't save them to csv
  
  # Save to csv
write.table(final, "C:\\Users\\brany\\Documents\\Revature\\Training\\Projects\\P2\\Test\\covid_19_data_clean.csv", sep = ",", row.names = FALSE, col.names = FALSE, quote = FALSE)



          ##### EXTRA #####
# Not using write.csv because you have less customizeability
# write.csv(dataframeToWrite, "C:\\PATH\\NameOfFile.csv", row.names = FALSE)
# write.table(dataframeToWrite, "C:\\PATH\\NameOfFile.csv", sep = ",", row.names = FALSE, col.names = FALSE, quote = FALSE)

### Other ways to combine countries, table is most efficient though
#install.packages("tidyverse")
#library(tidyverse)

## ddply
#library(plyr)
#library(reshape2)
#ddply(df, X1, numcolwise(sum))

## Using aggregate
#test_agg = aggregate(. ~ X1, data = df, FUN = sum)

#prefixes = unique(sub("\\..*", "", colnames(data)))
#sapply(prefixes, function(x)rowSums(data[,startsWith(colnames(data), x)]))

## Without having to transpose the data first
#t(rowsum(t(data), group = colnames(data), na.rm = T))


          ##### Initial #####
#ts_19_confirmed
# creates a transpose of data
transpose1 = t(ts_c19_confirmed)
# removes columns, Province/State, Lat, Long
remCol1 = transpose1[-c(1,3,4),]
# fixes row numbers after deleting rows
row.names(remCol1) <- NULL
# turn into dataframe
df = data.frame(remCol1)
# creates a list of new dates from old dates
newDateList = lapply(remCol1[1:nrow(remCol1)], ts_oldDate_to_newDate)
#replace old date in data frame with new date
df$X1=newDateList
df[1,1] = "Date"

# This line is specifically to be able to write the table
df_test = apply(df,2,as.character)
# All numbers are stored as characters, so we need to convert to numeric
df_test1 = df_test[,-1]
colnames(df_test1) <- NULL
# now we have a transposed table with country then the rest being the data
test1 = data.frame(t(df_test1))
# the columns we want to modify
i = c(2:ncol(test1))
# converts the columns to numeric
test1[, i] = apply(test1[ , i], 2, function(x) as.numeric(as.character(x)))

# convert into data.table
#install.packages("data.table", dependencies=TRUE)
library(data.table)
df_table = data.table(test1)
# sum up values by country
tableTest = df_table[, lapply(.SD, sum), by=X1]

finalCountries = data.frame(t(tableTest))
finalDate = data.frame(t(data.frame(newDateList)))

final = cbind(finalDate, finalCountries)
row.names(final) <- NULL
colnames(final) <- NULL

# Remove white space from data
library(dplyr)
library(stringr)
final %>%
  mutate(across(is.character), str_trim)

# Convert data frame to be written
ts_c19_confirmed_clean = apply(final,2,as.character)

write.table(ts_c19_confirmed_clean, "C:\\Users\\brany\\Documents\\Revature\\Training\\Projects\\P2\\Test\\ts_confirmed_byCountry.csv", sep = ",", row.names = FALSE, col.names = FALSE, quote = FALSE)
