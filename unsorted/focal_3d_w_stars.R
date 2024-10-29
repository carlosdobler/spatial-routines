    ss <- s[,1000:1200, 200:350]
    mm <- array(NA, c(dim(ss)[2], dim(ss)[1], 2))


    for (y_ in seq(dim(mm)[1]) %>% tail(-1) %>% head(-1)){ # latitude ---> columns
      for (x_ in seq(dim(mm)[2]) %>% tail(-1) %>% head(-1)){ # longitude ---> rows

        print(str_glue("{y_}, {x_}"))

        v <-
          ss[,(x_-1):(x_+1),(y_-1):(y_+1),] %>%
          pull() %>%
          as.vector()

        if (all(v == 0)) {
          p <- c(-9999,-9999)
        } else {

          v[v == 0] <- 0.001 # avoid errors in gamma fitting

          lmom <- lmom::samlmu(v)
          p <- lmom::pelgam(lmom) %>% unname()

        }

        mm[y_,x_,1] <- p[1]
        mm[y_,x_,2] <- p[2]

      }
    }

    mm[,,2] %>% starsExtra::matrix_to_stars() %>% plot()
