library(reticulate)
# conda_create("r-geedim")
use_condaenv("r-geedim")
# repl_python()
# >>> !pip install geedim
# see last line of https://rstudio.github.io/reticulate/articles/python_packages.html

gs <- import("geedim")


# PARAMETERS: 
collection <- "MODIS/061/MCD64A1"
start_date <- "2000-01-01"
end_date <- "2010-12-31"

down_dir <- "..."

# optional:
band <- "BurnDate"
rs_method <- "near"



# save a reference raster to use with geedim
r <- stringr::str_glue("{tempdir()}/reference_raster.tif")
stars::write_stars(reference_raster, r)


# geedim CALL

# only search
system(stringr::str_glue("geedim search -c {collection} -s {start_date} -e {end_date}"))

# download
# geedim search -c MODIS/061/MCD64A1 -s 2001-01-01 -e 2010-12-31 download --like /tmp/RtmpMUSYYc/reference_raster.tif -dd /mnt/pers_disk/modis_fire -bn BurnDate -rs near

