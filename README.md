Run the following script when starting a new project to copy modules into the project directory and be able to access them with `{box}`. Re-run every time a module or function changes to ensure everything is up-to-date.
Make sure to edit the "MODULES" section depending on the ones required by the project.

```
# MODULES
rts <- c("general_tools",
          "drought")

# *****

if (!fs::dir_exists("functions")) {
  fs::dir_create("functions")
}

purrr::walk(rts, \(rt){
  download.file(
    stringr::str_glue("https://raw.github.com/carlosdobler/spatial-routines/master/{rt}.R"), 
    stringr::str_glue("functions/{rt}.R"), 
    quiet = T
  )
})
