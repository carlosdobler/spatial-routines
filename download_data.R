
# reticulate::use_condaenv("risk")

rt_download_era_monthly <- function(var, yrs, mons, pls = NA, dest_dir) {

  # Function to download monthly ERA5 data from CDS. It downloads only 
  # one month (and one pressure level) at a time.
  
  # The cdsapi python package needs to be installed, and credentials
  # updated.
  
  # var = variable name (in CDS)
  # yrs, mons, pls = year(s), month(s), and pressure level(s) to download
  # (if pressure levels is left blank, single level data will be downloaded)
  # dest_dir = destination path (accepts gs buckets)
  
  
    
  mons <- stringr::str_pad(mons, 2, "left", "0")
  
  cdsapi <- reticulate::import("cdsapi")
  
  
  # if file is to be saved in bucket, save it first
  # in temporary directory
  if (stringr::str_sub(dest_dir, end = 2) == "gs") {
    dest <- tempdir()
  } else {
    dest <- dest_dir
  }
  
  
  for (yr in yrs) {
    for (mon in mons) {
      
      print(stringr::str_glue("Downloading {yr} {mon}"))
      
      # single level
      if (is.na(pls)) {
        
        file_name <- stringr::str_glue("era5_{stringr::str_replace(var, '_', '-')}_mon_{yr}-{mon}-01.nc")
        
        # while loop to retry if downloading fails
        
        a <- "a" # empty vector
        class(a) <- "try-error" # assign error class
        
        while(class(a) == "try-error") {
        
          a <- 
            try(
              cdsapi$Client()$retrieve(
                
                name = "reanalysis-era5-single-levels-monthly-means",
                
                request = reticulate::dict(format = "netcdf",
                                           product_type = "monthly_averaged_reanalysis",
                                           variable = var,
                                           year = yr,
                                           month = mon,
                                           time = "00:00"),
                
                target = stringr::str_glue("{dest}/{file_name}"))
            )
          
          if (class(a) == "try-error") {
            
            print(stringr::str_glue("  waiting to retry..."))
            Sys.sleep(3)
            
          }
        }
        
        # move 
        if (stringr::str_sub(dest_dir, end = 2) == "gs") {
          system(stringr::str_glue("gsutil mv {dest}/{file_name} {dest_dir}"))
        }
        
        
      # pressure levels
      } else {
        
        for (pl in pls) {
          
          file_name <- stringr::str_glue("era5_{stringr::str_replace(var, '_', '-')}-{pl}_mon_{yr}-{mon}-01.nc")
          
          # while loop to retry if downloading fails
          
          a <- "a" # empty vector
          class(a) <- "try-error" # assign error class
          
          while(class(a) == "try-error") {
            
            a <- 
              try(
                cdsapi$Client()$retrieve(
                  
                  name = "reanalysis-era5-pressure-levels-monthly-means",
                  
                  request = reticulate::dict(format = "netcdf",
                                             product_type = "monthly_averaged_reanalysis",
                                             variable = var,
                                             pressure_level = pl,
                                             year = yr,
                                             month = mon,
                                             time = "00:00"),
                  
                  target = stringr::str_glue("{dest}/{file_name}"))
              )
            
            if (class(a) == "try-error") {
              
              print(stringr::str_glue("  waiting to retry..."))
              Sys.sleep(3)
              
            }
          }
          
          if (stringr::str_sub(dest_dir, end = 2) == "gs") {
            system(stringr::str_glue("gsutil mv {dest}/{file_name} {dest_dir}"))
          }
          
        } # end of pressure levels loop
        
      }
      
    } # end of month loop
  } # end of year loop
  
}
