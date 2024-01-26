
# Function to correctly format a time dimension
# It returns a vector of dates of PCICt class

time_vector <- function(dates) {
  
  # dates = a string vector of dates with format "yyyy-mm-dd" or
  # a POSIX or PCICt vector of dates. 
  
  if (class(dates) != "character") {
    dates <- as.character(dates)
  }
  
  dates_formatted <- stringr::str_sub(dates, end = 10)
  
  # Obtain calendar type
  max_feb <- 
    dates_formatted[stringr::str_sub(dates_formatted, 6,7) == "02"] |> # filter feb months
    stringr::str_sub(9,10) |> # extract days
    as.numeric() |> 
    max()
  
  model_cal <- 
    dplyr::case_when(max_feb == 30 ~ "360_day",
                     max_feb == 29 ~ "gregorian",
                     max_feb == 28 ~ "noleap")
  
  print(stringr::str_glue("   Calendar type: {model_cal}"))
  
  # update time_vector
  time_vector <- PCICt::as.PCICt(dates_formatted, cal = model_cal)
  
  return(time_vector)
  
}
