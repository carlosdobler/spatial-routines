

ff <- 
  "/mnt/bucket_mine/era5/daily_aggregates/total_precipitation/" %>% 
  fs::dir_ls()

ff %>% 
  str_sub(-13,-4) %>% 
  as_date() %>% 
  year() %>% 
  unique()



# *********************************************************************************

url <- "https://object-store.os-api.cci2.ecmwf.int/cci2-prod-cache/8ded725de56bacdd7ff488f52a1a7a41.nc"
dest_file <- "precip_2021_2022.nc"

dir_data <- "/mnt/pers_disk/tmp"
fs::dir_create(dir_data)



# copy to terminal !!!
stringr::str_glue("wget -O {dir_data}/{dest_file} {url}")  


# ********************************************************************************


# dest_file <- "tas_2021.nc"
# funs <- c("max", "min", "mean") |> setNames(c("2m_maximum_temperature", "2m_minimum_temperature", "2m_temperature"))
# var_name <- "t2m"
# var_un <- "K"


dest_file <- "precip_2021_2022.nc"
funs <- c("sum") |> setNames("total_precipitation")
var_name <- "tp"
var_un <- "m/d"

# ********

library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
plan(multicore, workers = 6)
options(future.globals.maxSize = 1000 * 1024^2)
options(future.rng.onMisuse = "ignore")

source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")

dir_data <- "/mnt/pers_disk/tmp"
# fs::dir_delete(dir_data)
# fs::dir_create(dir_data)


f <- str_glue("{dir_data}/{dest_file}")

s_proxy <- 
  f %>% stars::read_ncdf(proxy = T)

time_vector <- 
  st_get_dimension_values(s_proxy, 3)

first(time_vector)
last(time_vector)



# time_vector_sub <-
#   seq(as_date("2008-03-01"), last(time_vector) %>% as_date(), by = "1 month")

time_vector_sub <-
  seq(first(time_vector) %>% as_date(), last(time_vector) %>% as_date(), by = "1 month")


first(time_vector_sub)
last(time_vector_sub)




walk(time_vector_sub, \(one_mon){
  
  time_vector_1mon <- 
    time_vector[year(time_vector) == year(one_mon) & month(time_vector) == month(one_mon)]
  
  dates_1mon <- unique(as_date(time_vector_1mon))
  
  tictoc::tic()
  s_days <- 
    future_map(dates_1mon, \(dy){
      
      t_in <- which(as_date(time_vector) == dy)
      
      s <- 
        f %>% 
        read_ncdf(ncsub = cbind(start = c(1,1,t_in[1]),
                                count = c(NA,NA, length(t_in)))) %>% 
        suppressMessages()
      
      t_tmp <- st_get_dimension_values(s,3)
      
      print(str_glue("{dy} :: {length(t_tmp)} :: {last(t_tmp)}"))
      
      ss <- 
        map(funs %>% set_names(), \(fun){
          
          s %>% 
            st_apply(c(1,2), fun, .fname = "v")
          
        })
    })
  tictoc::toc()
  
  foo <- 
    s_days %>% 
    transpose() %>%
    map(\(ss){
      
      do.call(c, c(ss, along = "time")) %>% 
        st_set_dimensions("time", values = dates_1mon) %>% 
        mutate(v = v %>% units::set_units(!!var_un)) %>% 
        setNames(var_name)
      
    })
  
  foo %>% 
    set_names(names(funs)) %>% 
    iwalk(\(s, i){
      
      ii <- str_replace_all(i, "_", "-")
      
      print(ii)
      
      final_name <- str_glue("{dir_data}/era5_{ii}_day_{first(dates_1mon)}.nc")
      
      rt_write_nc(
        s,
        final_name,
        calendar = "gregorian"
      )
      
      str_glue("gsutil mv {final_name} gs://clim_data_reg_useast1/era5/daily_aggregates/{i}/") %>% 
        system()#ignore.stdout = T, ignore.stderr = T)
      
    })
  
  rm(s_days, foo)
  gc()
  
})

