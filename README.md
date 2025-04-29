Run the following script when starting a new project to copy these routines into the directory of the project and being able to access them with `{box}`. Re-run every time a routine changes to ensure everything is up-to-date.
Make sure to edit the "ROUTINES" section depending on the ones required by the project.

```
# ROUTINES
funs <- c("general_tools",
          "drought")

# *****

if (!fs::dir_exists("functions")) {
  fs::dir_create("functions")
}

purrr::walk(funs, \(fun){
  download.file(stringr::str_glue("https://raw.github.com/carlosdobler/spatial-routines/master/{fun}.R"), 
                stringr::str_glue("functions/{fun}.R"), 
                quiet = T)
  })
```

