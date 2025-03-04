# TILING FRAMEWORK

rt_tile_table <- function(s, tile_size, land = NULL) {
  
  # Function to split a stars object (can be a proxy)
  # into tiles of a given size. Output is a table specifying 
  # where each tile starts and ends in terms of cell positions. 
  
  # If "land" is provided, an additional column is added indicating 
  # whether a tile covers a portion of land. In this case, the output 
  # is an sf object (tiles as polygons) with an associated table.
  
  # ARGUMENTS:
  # * s = stars object or proxy
  # * tile_size = number of cells per side of tile
  # * land = stars object in which land is represented with any 
  #   value and oceans as NA. This object *must* have the exact 
  #   same spatial dimensions as s.
  
  
  # dimensions' indices
  dims <- 
    c(1,2) |> 
    set_names(c("x", "y"))
  
  
  # create table of positions, one for each dimension
  df <- 
    imap(dims, function(dim_id, dim_name){
      
      d <- 
        dim(s)[dim_id] |> 
        seq_len()
      
      n <- 
        round(dim(s)[dim_id]/tile_size)
      
      split(d, ceiling(d/(length(d)/n))) |> 
        
        map_dfr(~tibble(start = first(.x),
                        end = last(.x)) |> 
                  mutate(count = end - start + 1)) |> 
        
        rename(!!str_glue("start_{dim_name}") := 1,
               !!str_glue("end_{dim_name}") := 2,
               !!str_glue("count_{dim_name}") := 3)
      
    })
  
  # combine both tables
  df <- 
    expand_grid(df$x, df$y) 
  
  # add index column with appropriate padding
  length_id <- 
    df |> 
    nrow() |> 
    str_length()
  
  df <- 
    df %>%
    mutate(tile_id = row_number() |> str_pad(length_id, "left", "0"), .before = 1)
  
  
  # if land is provided:
  if (!is.null(land)) {
    
    df <-
      # loop through tiles (rows of table)
      mutate(df, land = pmap_dfr(df, function(start_x, end_x, start_y, end_y, ...){
        
        # crop the land raster to the tile
        land_tile <- 
          land[,
               start_x:end_x,
               start_y:end_y]
        
        # turn to polygon
        pol_tile <-
          land_tile %>%
          st_bbox() %>%
          st_as_sfc() %>%
          st_sf()
        
        # evaluate if there is land in the cropped land tile
        pol_tile %>%
          mutate(land = if_else(all(is.na(pull(land_tile))), F, T))
        
      })) |> 
      unnest(cols = c(land)) |> 
      st_as_sf()
    
  }
  
  return(df)
  
}


# *****


rt_tile_load <- function(start_x, start_y, count_x, count_y, list_files, parallel = NULL) {
  
  # Helper function to load a tile of a list of files
  
  # NEEDS A WAY TO SPECIFY TIME STEPS TO IMPORT!
  # OR TO ACCOUNT FOR 1 TIME STEP
  
  
  if (!is.null(parallel)) {
    if (parallel) {
      oplan <- plan(multicore)
    } else {
      oplan <- plan(sequential)
    }
    
    on.exit(plan(oplan))
  }
  
  s_tile <-
    list_files |>
    future_map(\(f){
      
      read_ncdf(f,
                ncsub = cbind(start = c(start_x,
                                        start_y,
                                        1),
                              
                              count = c(count_x,
                                        count_y,
                                        NA)))
      
    }) |>
    suppressMessages() |>
    do.call(c, args = _)
  
  return(s_tile)
  
}


# *****


rt_tile_loop <- function(df_tiles, list_files, FUN, dir_tiles) {
  
  # Function that first loads data from list_files, then runs a 
  # function (FUN) on each tile in df_tiles, and then saves the 
  # output (a processed tile) in dir_tiles.
  
  
  
  
  # if dir_tiles does not exist, create it
  if (!fs::dir_exists(dir_tiles)) {
    fs::dir_create(dir_tiles)
  }
  
  # should rt_tile_h_load run in parallel?
  # depends on current plan
  if (is(plan(), "sequential")) {
    parallel_load <- T
  } else {
    parallel_load <- F
  }
  
  # loop through tiles
  future_pwalk(df_tiles, function(tile_id, start_x, start_y, count_x, count_y, ...){
    
    # load all data within the tile
    s_tile <-
      rt_tile_h_load(start_x, start_y, count_x, count_y, list_files, parallel_load)
    
    # apply function
    s_tile <-
      FUN(s_tile)
    
    # save tile
    s_tile |>
      write_rds(str_glue("{dir_tiles}/tile_{tile_id}.rds"))
    
  })
  
}


# *****


rt_tile_mosaic <- function(df_tiles, dir_tiles, spatial_dims, time_dim = NULL, time_lims = NULL) {
  
  # df_tiles, dir_tiles = paste0(dir_tiles, "/tile_1"), spatial_dims = st_dimensions(s_proxy), time_dim = t, time_lims = t_lim
  
  s <- 
    st_as_stars(dimensions = spatial_dims)
  
  df_tiles <- 
    df_tiles |> 
    st_drop_geometry()
  
  
  # if tiling accounts for land coverage
  if (df_tiles |> names() |> str_detect("land") |> any()) {
    
    rows <-
      # loop through rows
      map(unique(df_tiles$start_y), function(row_i) {
        
        one_row <- 
          df_tiles |>  
          filter(start_y == row_i) |>  
          future_pmap(function(tile_id, start_x, end_x, start_y, end_y, land, ...) {
            
            # if tile has no land
            # create an empty stars object of the size of the tile
            if (land == FALSE) {
              
              s <- s[, start_x:end_x, start_y:end_y]
              
              if (is.null(time_dim)) {
                
                empty_array <- 
                  array(NA, dim = c(dim(s)[1], 
                                    dim(s)[2]))
                
                
              } else {
                
                empty_array <- 
                  array(NA, dim = c(dim(s)[1], 
                                    dim(s)[2],
                                    time = length(time_dim)))
                
              }
              
              empty_stars <- 
                st_as_stars(empty_array)
              
              st_dimensions(empty_stars)[1] <- st_dimensions(s)[1]
              st_dimensions(empty_stars)[2] <- st_dimensions(s)[2]
              
              
              if (!is.null(time_dim)) {
                
                empty_stars <- 
                  st_set_dimensions(empty_stars, 3, values = time_dim)
                
              }
              
              return(empty_stars)
              
              # tile has land:
              # load from saved file
            } else {
              
              f_tile <-
                dir_tiles |>  
                fs::dir_ls(regexp = tile_id)
              
              if (is.null(time_lims)) {
                
                f_tile |> 
                  read_ncdf() |>  
                  suppressMessages()
                
              } else {
                
                f_tile |> 
                  read_ncdf(ncsub = cbind(start = c(1, 1, time_lims[1]),
                                          count = c(NA,NA, time_lims[2]))) |>  
                  suppressMessages()
                
              }
              
            }
            
          })
    
    # concatenate tiles from one row
    one_row <- 
      do.call(c, c(one_row, along = 1))
    
    # fix dimension
    st_dimensions(one_row)[1] <- st_dimensions(s)[1]
    
    return(one_row)
    
      })



# tiling does not account for land coverage:
# load all tiles from saved files
  } else {
    
    
    rows <-
      map(unique(df_tiles$start_y), function(row_i) {
        
        one_row <- 
          df_tiles |>  
          filter(start_y == row_i) |>  
          future_pmap(function(tile_id, ...) {
            
            dir_tiles |>  
              fs::dir_ls(regexp = tile_id) |> 
              read_ncdf() |>  
              suppressMessages()
            
          })
        
        st_dimensions(one_row)[1] <- st_dimensions(s)[1]
        
        return(one_row)
        
      })
    
  }
  
  
  
  # concatenate all rows
  mos <- 
    do.call(c, c(rows, along = 2))
  
  # fix dimension
  st_dimensions(mos)[2] <- st_dimensions(s)[2]
  
  return(mos)
  
}
