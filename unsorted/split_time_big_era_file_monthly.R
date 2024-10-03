url <- "https://object-store.os-api.cci2.ecmwf.int/cci2-prod-cache/5526e8b33210adf86bc47c3825e7b330.nc"
date_i <- "1970-01-01"
date_f <- "1970-12-01"
var <- "total_precipitation"


library(tidyverse)
library(stars)

source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")

# download big ERA5 file


url %>% 
  download.file(destfile = "/mnt/pers_disk/tmp/temp.nc", quiet = T, method = "wget")

dates <- 
  seq(as_date(date_i), as_date(date_f), by = "1 month")

iwalk(dates, \(x, i){
  
  print(x)
  
  a <- 
    read_ncdf("/mnt/pers_disk/tmp/temp.nc",
              ncsub = cbind(start = c(1,1,i),
                            count = c(NA,NA,1))) %>% 
    suppressMessages()
  
  ii <- st_get_dimension_values(a, 3) %>% as.character() %>% as_date() %>% identical(x)
  print(ii)
  
  rt_write_nc(adrop(a),
              str_glue("/mnt/pers_disk/tmp/era5_{str_replace(var, '_', '-')}_mon_{x}.nc"))
  
  "gsutil mv /mnt/pers_disk/tmp/era5_{str_replace(var, '_', '-')}_mon_{x}.nc gs://clim_data_reg_useast1/era5/monthly_means/{var}/" %>% 
    str_glue() %>% 
    system()
  
})

fs::file_delete("/mnt/pers_disk/tmp/temp.nc")
