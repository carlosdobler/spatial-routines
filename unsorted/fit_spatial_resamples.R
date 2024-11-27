tb_pred <-
  folds %>% # folds tb from e.g. spatialsample::spatial_block_cv
  mutate(.preds = map(splits, \(df){
    
    mod <- 
      fit(wf, # a workflow
          data = analysis(df) %>% st_drop_geometry())
    
    # identify the assessment set
    holdout <- 
      assessment(df)
    
    # return the assessment set, with true and predicted values
    tibble(# geometry = holdout$geometry,
      presence = holdout$presence
    ) %>% 
      bind_cols(predict(mod, holdout, type = "prob"))
    
  }))


tb <- 
  tb_pred %>% 
  select(-splits) %>% 
  unnest(.preds)

# assess accuracy
roc_auc(tb, .pred_1, truth = presence, event_level = "second")
accuracy(tb %>% 
           mutate(.pred_class = if_else(.pred_1 >= 0.5, 1, 0) %>% factor()),
         .pred_class, truth = presence)
