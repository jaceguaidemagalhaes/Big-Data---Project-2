package database

object Resources {
  // workingPath should be universal as long as "resources" folder is in main
  val workingPath = "resources/"

  // Original, unedited data
    // SNo, ObservationDate, Province/State, Country/Region, Last Update, Confirmed, Deaths, Recovered
  val covid_19_data: String = workingPath + "original/covid_19_data.csv"
    // All Time Series have hundreds of columns that span from 1/22/20 to 5/2/21
    // Province/State, Country/Region, Lat, Long, 1/22/20 - 5/2/21
  val ts_c19_confirmed: String = workingPath + "original/time_series_covid_19_confirmed.csv"
    // UID, iso2, iso3, code3, FIPS, Admin2, Province_State, Country_Region, Lat, Long_, Combined_Key, 1/22/2 - 5/2/21
  val ts_c19_confirmed_us: String = workingPath + "original/time_series_covid_19_confirmed_US.csv"
    // Province/State, Country/Region, Lat, Long, 1/22/20 - 5/2/21
  val ts_c19_deaths: String = workingPath + "original/time_series_covid_19_deaths.csv"
    // UID, iso2, iso3, code3, FIPS, Admin2, Province_State, Country_Region, Lat, Long_, Combined_Key, Population, 1/22/20 - 5/2/21
  val ts_c19_deaths_us: String = workingPath + "original/time_series_covid_19_deaths_US.csv"
    // Province/State, Country/Region, Lat, Long, 1/22/20 - 5/2/21
  val ts_c19_recovered: String = workingPath + "original/time_series_covid_19_recovered.csv"


  // Cleaned data
    // covid_19_data
  val covid_19_data_clean: String = workingPath + "clean/covid_19_data_clean.csv"
  // Following time series are transposed and grouped by Country (State for US)
    // time_series_covid_19_confirmed
  val ts_confirmed_byCountry: String = workingPath + "clean/ts_confirmed_byCountry.csv"
    // time_series_covid_19_confirmed_US
  val ts_confirmed_US_byState: String = workingPath + "clean/ts_confirmed_US_byState.csv"
    // time_series_covid_19_deaths
  val ts_deaths_byCountry: String = workingPath + "clean/ts_deaths_byCountry.csv"
    // time_series_covid_19_deaths_US
  val ts_deaths_US_byState: String = workingPath + "clean/ts_deaths_US_byState.csv"
    // time_series_covid_19_recovered
  val ts_recovered_byCountry: String = workingPath + "clean/ts_recovered_byCountry.csv"

    //
  val totalPopByCountry: String = workingPath + "clean/totalPopByCountry20202021.csv"
}
