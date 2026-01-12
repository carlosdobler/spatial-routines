#' List files in a google bucket directory
#'
#' @description
#' Function to list all files in a google cloud bucket
#' directory
#'
#' @param dir google cloud bucket directory
#'
#' @export
rt_gs_list_files <- function(dir) {
  paste0("gcloud storage ls ", dir) |>
    system(intern = T, ignore.stderr = T)
}


#' Download file(s) from a google bucket directory
#'
#' @description
#' Function to download one or multiple files from a google cloud bucket to a local
#' directory. When assigned to an object, that object will contain the updated file
#' name(s) (with path reflecting the destination directory). The function downloads
#' files in parallel by default using either mirai or future, depending on whether
#' daemons or a plan (e.g. multicore) have been called beforehand. If no daemons or
#' plan has been specified, downloads will be sequential. The option to force downloads
#' to be sequential is also available.
#'
#' @param f file(s) in the bucket to download (with full path; e.g. gs://...)
#' @param dest local destination directory
#' @param quiet (boolean) if FALSE (default), omits printing whether files are being downloaded in parallel
#' @param parallel (boolean) TRUE downloads in parallel with mirai (if daemons have already been called) or
#' with future (if a plan has been specified). FALSE forces sequential downloads.
#' @param gsutil (boolean) if FALSE (default), downloads with "gcloud storage" instead of "gsutil"
#'
#' @export

rt_gs_download_files <- function(
  f,
  dest,
  quiet = FALSE,
  parallel = TRUE,
  gsutil = FALSE,
  update_only = FALSE
) {
  #
  if (!update_only) {
    # create destination directory if it does not exist
    if (!dir.exists(dest)) {
      dir.create(dest, recursive = TRUE)
    }

    # check whether the path is a google cloud one
    if (any(substr(f, 1, 2) != "gs")) {
      # if path is wrong (not in google cloud), print error message
      print(paste0("ERROR: not a google cloud directory"))
      #
    } else {
      # if path is correct, proceed
      # identify the parallel engine to use ("m" for mirai, "f" for future)
      parallel_ <- "none"
      if (parallel) {
        if (
          requireNamespace("mirai", quietly = TRUE) &&
            mirai::status()$connections > 0
        ) {
          parallel_ <- "m"
        } else if (
          requireNamespace("furrr", quietly = TRUE) &&
            !is(future::plan(), "sequential")
        ) {
          parallel_ <- "f"
        } else {
          parallel_ <- "none"
        }
      }
      # check whether gcloud or gsutil utility will be used
      if (gsutil) {
        cmd <- "gsutil"
      } else {
        cmd <- "gcloud storage"
      }

      f_down <- function(f_) {
        paste0(cmd, " cp ", f_, " ", dest) |>
          system(ignore.stdout = TRUE, ignore.stderr = TRUE)
      }

      # download files
      if (parallel_ == "none") {
        # sequential download
        if (!quiet) {
          message("   downloading sequentially...")
        }

        for (f_ in f) {
          f_down(f_)
        }
        #
      } else if (parallel_ == "m") {
        # parallel download with mirai
        if (!quiet) {
          message("   downloading in parallel (mirai)...")
        }

        f |>
          purrr::walk(
            purrr::in_parallel(
              \(fi) f_down(fi),
              f_down = f_down,
              cmd = cmd,
              dest = dest
            )
          )
      } else if (parallel_ == "f") {
        # parallel download with future
        if (!quiet) {
          message("   downloading in parallel (future)...")
        }

        f |>
          furrr::future_walk(f_down)
      }
    }
  }

  # update file names to reflect their new local path
  updated <-
    paste0(dest, "/", basename(f))

  return(updated)
}


#' Save stars objects into NetCDFs
#'
#' @description
#' Function to write stars objects as NetCDFs. It can save
#' objects with multiple variables, preserving their units.
#'
#' @param stars_obj a stars object with either two or more
#' dimensions; lon (x) and lat (y) must be the first two, in that order.
#' @param filename where should the file be saved?
#' @param calendar one of "360_day", "gregorian", or "noleap". If not
#' provided, it will guess from the stars obj (needs February months to work)
#' @param gatt_name global attribute name
#' @param gatt_val global attribute value (or text)
#' @param ... additional arguments
#'
#' @export
rt_write_nc <- function(
  stars_obj,
  filename,
  calendar = NA,
  gatt_name = NA,
  gatt_val = NA,
  ...
) {
  # create a list to store dimensions
  dims <- vector("list", length(dim(stars_obj)))
  names(dims) <- names(dim(stars_obj))

  # define lon dimension
  dims[[1]] <-
    ncdf4::ncdim_def(
      name = names(dims)[1],
      units = "degrees_east",
      vals = stars::st_get_dimension_values(stars_obj, 1)
    )

  # define lat dimension
  dims[[2]] <-
    ncdf4::ncdim_def(
      name = names(dims)[2],
      units = "degrees_north",
      vals = stars::st_get_dimension_values(stars_obj, 2)
    )

  # define other dimensions
  if (length(dims) > 2) {
    for (dim_i in seq_along(dims) |> tail(-2)) {
      dim_vals <-
        stars::st_get_dimension_values(stars_obj, dim_i)

      # if dimension is time, handle calendars
      if (
        inherits(dim_vals, "Date") |
          inherits(dim_vals, "POSIXct") |
          inherits(dim_vals, "PCICt")
      ) {
        time_vector_str <-
          substr(as.character(dim_vals), 1, 10)

        if (all(diff(dim_vals) == 1)) {
          # if the time difference is 1 day, then the calendar
          # can be guessed if not provided
          if (!is.na(calendar)) {
            cal_spec <- calendar
          } else {
            # get february days to guess calendar
            feb <-
              time_vector_str[substr(time_vector_str, 6, 7) == "02"]

            if (length(feb) > 1) {
              max_feb <-
                max(as.numeric(substr(feb, 9, 10)))

              cal_spec <-
                if (max_feb == 30) {
                  "360_day"
                } else if (max_feb == 29) {
                  "gregorian"
                } else if (max_feb == 28) {
                  "noleap"
                }
            } else {
              message("ERROR: no multiple Februaries: cannot guess calendar")
            }
          }

          # create PCICt vector
          time_vector <-
            PCICt::as.PCICt(time_vector_str, cal = cal_spec)
        } else {
          # if time difference is not 1 day, assume gregorian
          time_vector <-
            PCICt::as.PCICt(time_vector_str, cal = "gregorian")
        }

        # get calendar name from PCICt object
        cal_attr <- attributes(time_vector)$cal
        cal <-
          if (cal_attr == "360") {
            "360_day"
          } else if (cal_attr == "365") {
            "365_day"
          } else if (cal_attr == "proleptic_gregorian") {
            "gregorian"
          }

        # define time dimension
        dims[[dim_i]] <-
          ncdf4::ncdim_def(
            name = names(dims)[dim_i],
            units = "days since 1970-01-01",
            vals = as.numeric(time_vector) / 86400,
            calendar = cal
          )
      } else {
        # if dimension is not time, create a simple dimension
        dims[[dim_i]] <-
          ncdf4::ncdim_def(
            name = names(dims)[dim_i],
            units = "",
            vals = dim_vals
          )
      }
    }
  }

  # get variable names
  var_names <- names(stars_obj)

  # get variable units
  var_units <-
    sapply(seq_along(var_names), function(x) {
      if (inherits(stars_obj[[x]], "units")) {
        un <- units::deparse_unit(stars_obj[[x]])
      } else {
        un <- ""
      }
      return(un)
    })

  # define variables for the NetCDF
  varis <-
    mapply(
      function(vn, vu) ncdf4::ncvar_def(name = vn, units = vu, dim = dims, ...),
      var_names,
      var_units,
      SIMPLIFY = FALSE
    )

  # create NetCDF file
  ncnew <-
    ncdf4::nc_create(filename = filename, vars = varis, force_v4 = TRUE)

  # add global attributes, if any
  if (!is.na(gatt_name)) {
    ncdf4::ncatt_put(ncnew, varid = 0, attname = gatt_name, attval = gatt_val)
  }

  # write data to the NetCDF file
  invisible(
    sapply(
      seq_along(var_names),
      function(vn) {
        ncdf4::ncvar_put(
          nc = ncnew,
          varid = varis[[vn]],
          vals = stars_obj[[vn]]
        )
      }
    )
  )

  # close the NetCDF file
  ncdf4::nc_close(ncnew)
}


#' Get cell position (index) from coordinates
#'
#' @description
#'  Function to obtain the position of cells in a grid
#'  given a set of coordinates. If four coordinates
#'  are provided, then the functions outputs start positions,
#'  end positions, and counts.
#'
#' @export

rt_from_coord_to_ind <- function(stars_obj, xmin, ymin, xmax = NA, ymax = NA) {
  # check if a single point or a bounding box is provided
  if (is.na(xmax) & is.na(ymax)) {
    # if a single point, get the closest cell indices
    coords <-
      mapply(
        function(coord, dim_id) {
          # get dimension values
          s <-
            stars_obj |>
            stars::st_get_dimension_values(dim_id)

          # handle longitude conversion if necessary
          if (dim_id == 1 & max(s) > 180 & coord < 0) {
            which.min(abs(s - (360 + coord)))
          } else {
            which.min(abs(s - coord))
          }
        },
        c(xmin, ymin),
        c(1, 2),
        SIMPLIFY = FALSE
      )

    names(coords) <- c("x", "y")

    # prepare the output list
    r <-
      list(x = coords$x, y = coords$y)
    #
  } else {
    # if a bounding box, get start and end indices
    coords <-
      mapply(
        function(coords, dim_id) {
          # get dimension values
          s <-
            stars_obj |>
            stars::st_get_dimension_values(dim_id)

          # handle longitude conversion if necessary
          if (dim_id == 1 & max(s) > 180 & any(coords < 0)) {
            lapply(coords, function(x) which.min(abs(s - (360 + x))))
          } else {
            lapply(coords, function(x) which.min(abs(s - x)))
          }
        },
        list(c(xmin, xmax), c(ymin, ymax)),
        c(1, 2),
        SIMPLIFY = FALSE
      ) |>
      unlist(recursive = FALSE)

    names(coords) <- c("xmin", "xmax", "ymin", "ymax")

    # ensure y_start is smaller than y_end
    if (coords$ymax > coords$ymin) {
      y_start <- coords$ymin
      y_end <- coords$ymax
      y_count <- coords$ymax - coords$ymin + 1
    } else {
      y_start <- coords$ymax
      y_end <- coords$ymin
      y_count <- coords$ymin - coords$ymax + 1
    }

    # prepare the output list with start, end, and count for each dimension
    r <-
      list(
        x_start = coords$xmin,
        y_start = y_start,
        x_end = coords$xmax,
        y_end = y_end,
        x_count = coords$xmax - coords$xmin + 1,
        y_count = y_count
      )
  }

  return(r)
}
