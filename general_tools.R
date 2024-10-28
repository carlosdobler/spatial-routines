
rt_write_nc <- function(stars_obj, filename, daily = T, calendar = NA, gatt_name = NA, gatt_val = NA) {
  
  # Function to save stars objects into netCDFs. It can save 
  # objects with multiple variables. If variables have units, 
  # they will be saved.
  
  # ARGUMENTS:
  # * stars_obj = a stars object with either two or three 
  # dimensions: lon (x), lat (y), and time (optional), in that order.
  # * filename = where should the file be saved?
  # * daily = indicates whether the time dimension is daily
  # * calendar = one of "360_day", "gregorian", or "noleap". If not
  # provided, it will guess from the stars obj (needs February to work)
  # * gatt_name = global attribute name
  # * gatt_val = global attribute value (or text)
  
  
  
  # *******************************************************
  
  # define dimensions
  dim_lon <- ncdf4::ncdim_def(name = names(stars::st_dimensions(stars_obj)[1]), 
                              units = "degrees_east", 
                              vals = stars_obj |> 
                                stars::st_get_dimension_values(1))
  
  dim_lat <- ncdf4::ncdim_def(name = names(stars::st_dimensions(stars_obj)[2]), 
                              units = "degrees_north", 
                              vals = stars_obj |> 
                                stars::st_get_dimension_values(2))
  
  
  # if the stars object has a time dimension:
  if (length(dim(stars_obj)) > 2) {
    
    
    # format time vector
    
    dates <- 
      stars_obj |> 
      stars::st_get_dimension_values(3)
    
    if (class(dates)[1] != "character") {
      dates <- as.character(dates)
    }
    
    dates_formatted <- 
      stringr::str_sub(dates, end = 10)
    
    # if dates are not daily:
    if (!daily) {
      
      time_vector <- 
        PCICt::as.PCICt(dates_formatted, cal = "gregorian")
      
      # if dates are daily:
    } else if(daily) {
      
      if(!is.na(calendar)) {
        
        model_cal <- calendar
        
      } else {
        
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
        
        # print(stringr::str_glue("   Calendar type (daily): {model_cal}"))
        
      }
      
      time_vector <- 
        PCICt::as.PCICt(dates_formatted, cal = model_cal)
      
    }
    
    cal <- 
      dplyr::case_when(attributes(time_vector)$cal == "360" ~ "360_day",
                       attributes(time_vector)$cal == "365" ~ "365_day",
                       attributes(time_vector)$cal == "proleptic_gregorian" ~ "gregorian")
    
    dim_time <- ncdf4::ncdim_def(name = "time", 
                                 units = "days since 1970-01-01", 
                                 vals = as.numeric(time_vector)/86400,
                                 calendar = cal)
    
  }
  
  
  # define variables
  n <- names(stars_obj)
  
  if (length(dim(stars_obj)) > 2) {
    
    u <- purrr::map_chr(seq_along(n), function(x) {
      
      un <- try(units::deparse_unit(dplyr::pull(stars_obj[x,1,1,1])), silent = T)
      if (class(un) == "try-error") un <- ""
      return(un)
      
    })
    
    varis <- 
      purrr::map2(n, u, 
                  ~ncdf4::ncvar_def(name = .x,
                                    units = .y,
                                    dim = list(dim_lon, dim_lat, dim_time)))
    
  } else {
    
    u <- purrr::map_chr(seq_along(n), function(x) {
      
      un <- try(units::deparse_unit(dplyr::pull(stars_obj[x,1,1])), silent = T)
      if (class(un) == "try-error") un <- ""
      return(un)
      
    })
    
    varis <- 
      purrr::map2(n, u, 
                  ~ncdf4::ncvar_def(name = .x,
                                    units = .y,
                                    dim = list(dim_lon, dim_lat)))
    
  }
  
  # create file
  ncnew <- ncdf4::nc_create(filename = filename, 
                            vars = varis,
                            force_v4 = TRUE)
  
  
  # global attribute
  if (!is.na(gatt_name)) {
    ncdf4::ncatt_put(ncnew,
                     varid = 0,
                     attname = gatt_name,
                     attval = gatt_val)
  }
  
  
  
  # write data
  purrr::walk(seq_along(n),
              ~ncdf4::ncvar_put(nc = ncnew, 
                                varid = varis[[.x]], 
                                vals = stars_obj |> dplyr::select(all_of(.x)) |> dplyr::pull()))
  
  ncdf4::nc_close(ncnew)
  
}


# *****


rt_from_coord_to_ind <- function(stars_obj, xmin, ymin, xmax = NA, ymax = NA) {
  
  # Function to obtain the position of cells in a grid
  # given a set of coordinates. If four coordinates
  # are provided, then the functions outputs start and
  # count positions.
  
  if (is.na(xmax) & is.na(ymax)) {
    
    coords <- 
      map2(c(xmin, ymin), c(1,2), \(coord, dim_id){
        
        stars_obj %>% 
          st_get_dimension_values(dim_id) %>% 
          {. - coord} %>% 
          abs() %>% 
          which.min()
        
      }) %>% 
      set_names(c("x", "y"))
    
    r <- 
      list(x = coords$x,
           y = coords$y)
    
  } else {
    
    coords <- 
      map2(c(xmin, ymin, xmax, ymax), c(1,2,1,2), \(coord, dim_id){
        
        stars_obj %>% 
          st_get_dimension_values(dim_id) %>% 
          {. - coord} %>% 
          abs() %>% 
          which.min()
        
      }) %>% 
      set_names(c("xmin", "ymin", "xmax", "ymax"))
    
    
    if(coords$ymax > coords$ymin) {
      
      r <- 
        list(x_start = coords$xmin,
             y_start = coords$ymin,
             x_count = coords$xmax - coords$xmin + 1,
             y_count = coords$ymax - coords$ymin + 1)  
      
    } else {
      
      r <- 
        list(x_start = coords$xmin,
             y_start = coords$ymax,
             x_count = coords$xmax - coords$xmin + 1,
             y_count = coords$ymin - coords$ymax + 1)
      
    }
    
  }
  
  return(r)
}
