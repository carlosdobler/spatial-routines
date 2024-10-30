polygons_df <- 
  stars_obj %>% 
  st_as_sf(as_points = F, merge = T) %>% 
  st_coordinates() %>%
  as_tibble()
