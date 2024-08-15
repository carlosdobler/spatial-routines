# Saves a stars object into an netcdf file

write_nc <- function(stars_obj, t_vector, filename){

print("DEPRECATED")
  
  # stars_obj = a stars object with exactly 3 dimensions:
  # lon (x), lat (y), and time, in that order.
  # It can save objects with multiple variables.
  # If variables have units, they will be saved.

  
  # time_vector should be a PCICt object.
  # You can obtain it by running the time_vector.R script
  
  # filename = where should the file be saved?
  
  
  # **************************************************
  
  # define dimensions
  dim_lon <- ncdf4::ncdim_def(name = "lon", 
                              units = "degrees_east", 
                              vals = stars_obj |> 
                                stars::st_get_dimension_values(1))
  
  dim_lat <- ncdf4::ncdim_def(name = "lat", 
                              units = "degrees_north", 
                              vals = stars_obj |> 
                                stars::st_get_dimension_values(2))
  
  cal <- 
    dplyr::case_when(attributes(t_vector)$cal == "360" ~ "360_day",
                     attributes(t_vector)$cal == "365" ~ "365_day",
                     attributes(t_vector)$cal == "proleptic_gregorian" ~ "gregorian")
  
  dim_time <- ncdf4::ncdim_def(name = "time", 
                               units = "days since 1970-01-01", 
                               vals = as.numeric(t_vector)/86400,
                               calendar = cal)
  
  # define variables
  n <- names(stars_obj)
  u <- purrr::map_chr(seq_along(n), function(x) {
    
    un <- try(units::deparse_unit(dplyr::pull(stars_obj[x,1,1,1])), silent = T)
    if (class(un) == "try-error") un <- ""
    return(un)
  } 
  )
   
  varis <- 
    purrr::map2(n, u, 
                ~ncdf4::ncvar_def(name = .x,
                                  units = .y,
                                  dim = list(dim_lon, dim_lat, dim_time)))
  
  # create file
  ncnew <- ncdf4::nc_create(filename = filename, 
                            vars = varis,
                            force_v4 = TRUE)
  
  # write data
  purrr::walk(seq_along(n),
              ~ncdf4::ncvar_put(nc = ncnew, 
                                varid = varis[[.x]], 
                                vals = stars_obj |> dplyr::select(all_of(.x)) |> dplyr::pull()))
  
  ncdf4::nc_close(ncnew)
  
}
