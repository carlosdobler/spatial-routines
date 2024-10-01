library(tidyverse)
library(stars)

source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")

# download big ERA5 file
"https://object-store.os-api.cci2.ecmwf.int/cci2-prod-cache/ff0d06f2a6b90cad51a5a67e61354b11.nc" %>% 
  download.file(destfile = "/mnt/pers_disk/era5_pot_evap.nc", quiet = T, method = "wget")

dates <- 
  seq(as_date("1986-01-01"), as_date("2024-08-01"), by = "1 month")

iwalk(dates, \(x, i){
  
  print(x)
  
  a <- 
    read_ncdf("/mnt/pers_disk/era5_pot_evap.nc",
              ncsub = cbind(start = c(1,1,i),
                            count = c(NA,NA,1))) %>% 
    suppressMessages()
  
  ii <- st_get_dimension_values(a, 3) %>% as.character() %>% as_date() %>% identical(x)
  print(ii)
  
  rt_write_nc(adrop(a),
              str_glue("/mnt/pers_disk/era5_potential-evaporation_mon_{x}.nc"))
  
  "gsutil mv /mnt/pers_disk/era5_potential-evaporation_mon_{x}.nc gs://clim_data_reg_useast1/era5/monthly_means/potential_evaporation/" %>% 
    str_glue() %>% 
    system(ignore.stdout = T, ignore.stderr = T)
  
})

fs::file_delete("/mnt/pers_disk/era5_pot_evap.nc")
