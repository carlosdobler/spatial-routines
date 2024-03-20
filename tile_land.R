# Function to evaluate tiles that cover land
# tiles_tb = table generated with the tile.R script
# land = stars object where 1 = land, NA = ocean. It must 
# have the exact same dimensions as the proxy stars obj
# used for the tile.R (use st_warp if that's not the case).

fn_tile_land <- function(tiles_tb, land) {
      
      tiles_tb %>% 
        mutate(r = row_number()) %>%
        pmap_dfr(function(start_x, end_x, start_y, end_y, r) {
          
          land_tile <- 
            land[,
                 start_x:end_x,
                 start_y:end_y]
          
          pol_tile <- 
            land_tile %>% 
            st_bbox() %>% 
            st_as_sfc() %>% 
            st_sf() %>% 
            mutate(r = {{r}})
          
          pol_tile %>% 
            mutate(cover = if_else(all(is.na(pull(land_tile))), F, T))
          
        })
      
    }
