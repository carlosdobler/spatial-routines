reticulate::use_condaenv("risk")
library(reticulate)
library(tidyverse)

cdsapi <- import("cdsapi")

yrs <- as.character(c(1940:2023))
p_l <- "200"
var <- c("v_component_of_wind", # cds name
         str_glue("mean-vwind-{p_l}")) # my name


for (yr in yrs) {
  
  print(yr)
  
  if (is.null(p_l)) {
    
    cdsapi$Client()$retrieve(
      
      name = 
        "reanalysis-era5-single-levels-monthly-means",
      
      request = dict(format = "netcdf",
                     product_type = "monthly_averaged_reanalysis",
                     variable = var[1],
                     year = yr,
                     month = stringr::str_pad(1:12, 2, "left", "0"),
                     time = "00:00"),
      
      target = stringr::str_glue("/mnt/pers_disk/era5_mon_{var[2]}_{yr}.nc"))
    
  } else {
    
    cdsapi$Client()$retrieve(
      
      name = 
        "reanalysis-era5-pressure-levels-monthly-means",
      
      request = dict(format = "netcdf",
                     product_type = "monthly_averaged_reanalysis",
                     variable = var[1],
                     pressure_level = p_l,
                     year = yr,
                     month = stringr::str_pad(1:12, 2, "left", "0"),
                     time = "00:00"),
      
      target = stringr::str_glue("/mnt/pers_disk/era5_mon_{var[2]}_{yr}.nc"))
    
  }
  
}


fs::dir_create(str_glue("/mnt/bucket_mine/era/monthly/{var[2]}"))

"/mnt/pers_disk/" %>% 
  fs::dir_ls(regexp = var[2]) %>% 
  walk(\(f) {
    
    f_gs <- str_replace(f, "/mnt/pers_disk", str_glue("gs://clim_data_reg_useast1/era/monthly/{var[2]}"))
    
    str_glue("gsutil mv {f} {f_gs}") %>% system()
    
  })




