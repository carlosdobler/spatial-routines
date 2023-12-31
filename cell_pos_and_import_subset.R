# identify the column or row of a given coordinate

fn_get_cell_pos <- 
  
  function(star_obj, dim_id, coord) {
    
    star_obj %>% 
      st_get_dimension_values(dim_id) %>% 
      {. - coord} %>% 
      abs() %>% 
      which.min()
    
  }


# *****************


# import a spatial subset

fn_import_subset <- 
  
  function(file, xmin, xmax, ymin, ymax) {
    
    s_proxy <- read_stars(file, proxy = T)
    
    lon_start <- fn_get_cell_pos(s_proxy, 1, xmin)
    lon_count <- fn_get_cell_pos(s_proxy, 1, xmax) - lon_start
    
    lat_start <- fn_get_cell_pos(s_proxy, 2, ymax)
    lat_count <- fn_get_cell_pos(s_proxy, 2, ymin) - lat_start
    
    
    s <- 
      read_stars(file, 
                 RasterIO = list(nXOff = lon_start,
                                 nXSize = lon_count,
                                 nYOff = lat_start,
                                 nYSize = lat_count),
                 proxy = F)
    
    return(s)
    
  }
