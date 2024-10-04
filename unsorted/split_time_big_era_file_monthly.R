
url <- "https://object-store.os-api.cci2.ecmwf.int/cci2-prod-cache/faafd0feb766e1dede938ae08dbed968.nc"
date_i <- "2021-01-01"
date_f <- "2024-08-01"
var <- "2m_temperature" # with _


library(tidyverse)
library(stars)

source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")

# download big ERA5 file

dir_data <- "/mnt/pers_disk/tmp"
fs::dir_delete(dir_data)
fs::dir_create(dir_data)

url %>% 
  download.file(destfile = str_glue("{dir_data}/temp.nc"), quiet = F, method = "wget")

dates <- 
  seq(as_date(date_i), as_date(date_f), by = "1 month")

iwalk(dates, \(x, i){
  
  print(x)
  
  a <- 
    read_ncdf(str_glue("{dir_data}/temp.nc"),
              ncsub = cbind(start = c(1,1,i),
                            count = c(NA,NA,1))) %>% 
    suppressMessages()
  
  ii <- st_get_dimension_values(a, 3) %>% as.character() %>% as_date() %>% identical(x)
  print(ii)
  
  file_name <- str_glue("{dir_data}/era5_{str_replace(var, '_', '-')}_mon_{x}.nc")
  
  rt_write_nc(adrop(a),
              file_name)
  
  "gsutil mv {file_name} gs://clim_data_reg_useast1/era5/monthly_means/{var}/" %>% 
    str_glue() %>% 
    system()
  
})

fs::file_delete("/mnt/pers_disk/tmp/temp.nc")
