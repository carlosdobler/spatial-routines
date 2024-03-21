# Function to generate a table indexing cell positions of tiles of tile_size
# s = stars object

fn_tile <- function(s, tile_size) {
  
  tb <- 
    imap(c(1,2) %>% set_names(c("x", "y")), function(dim_id, dim_name){
      
      d <- 
        dim(s)[dim_id] %>%
        seq_len()
      
      n <- 
        round(dim(s)[dim_id]/tile_size)
      
      l <-
        split(d, 
              ceiling(d/(length(d)/n))) %>% 
        map_dfr(~tibble(start = first(.x), end = last(.x))) %>% 
        rename(!!str_glue("start_{dim_name}") := 1,
               !!str_glue("end_{dim_name}") := 2)
      
      return(l)
      
    })
  
  expand_grid(tb$x, tb$y) %>%
    mutate(tile_id = row_number() %>% str_pad(width = 8, "left", "0"))
  
}
