# s = reference stars obj

land_p <- 
  "/mnt/bucket_mine/misc_data/ne_110m_land/" %>% 
  read_sf()

land_r <- 
  land_p %>% 
  mutate(a = 1) %>% 
  select(a) %>% 
  st_rasterize(
    st_as_stars(st_bbox(),
                xlim = c(st_bbox(s)[1] - 180, st_bbox(s)[3] - 180),
                ylim = c(st_bbox(s)[2], st_bbox(s)[4]),
                dx = 0.11,
                values = 0)
  ) %>%
  st_warp(st_as_stars(st_bbox(),
                      xlim = c(st_bbox(s)[1], st_bbox(s)[3]),
                      ylim = c(st_bbox(s)[2], st_bbox(s)[4]),
                      dx = 0.11,
                      values = 0))

land_r <- 
  land_r %>% 
  st_warp(s,
          method = "max",
          use_gdal = T) %>% 
  setNames("land")

land_r[land_r == 0] <- NA

st_dimensions(land_r) <- st_dimensions(s)[1:2]

# ******

# V2

# remove antartica:
centr <- 
  land_pol |> 
  st_centroid() |> 
  st_coordinates() |> 
  as_tibble()

land_pol <- "/mnt/bucket_mine/misc_data/physical/ne_110m_land/ne_110m_land.shp" |> st_read()

land_r <- 
  land_pol %>%
  mutate(Y = centr$Y) |> 
  filter(Y > -60) |> 
  mutate(a = 1) %>% 
  select(a) %>% 
  st_rasterize(st_as_stars(st_bbox(),
                           dx = 0.1,
                           values = 0))

land_r <- 
  land_r %>% 
  st_warp(s[,,,1] |> st_as_stars() |> adrop(),
          method = "max",
          use_gdal = T) %>% 
  setNames("land")

land_r[land_r == 0] <- NA

st_dimensions(land_r) <- st_dimensions(s)[1:2]
