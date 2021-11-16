
# download data and check for failed or skipped query
download_data = function(database_id, var, ctry){
  
  suppressWarnings(rm(down_data, available_dataset, err))
  rows_to_add = error_row = c()
  
  
  oo = capture.output(
    err <- try(
      down_data <- imf_data(
        database_id,
        indicator = var,
        country = ctry,
        start = 1900,
        end = current_year(),
        freq = "A",
        return_raw = T,
        print_url = FALSE,
        times = 5
      ), silent = T,
    ), silent = T)
  
  if (class(err) == "try-error"){
    
    error_row = data.frame(`@INDICATOR` = var,
                           `@REF_AREA` = ctry,
                           status = "DOWNLOAD_FAILED", stringsAsFactors = F) %>%
                  setNames(gsub("X.", "@", names(.)))
    
  } else {
    
    available_dataset = down_data$CompactData$DataSet$Series
    
    if (!is.null(available_dataset)){
      
      if (is.null(nrow(available_dataset))){
        Obs_data = tibble(available_dataset[[6]])
        if (ncol(Obs_data) == 1){Obs_data = available_dataset[[6]] %>%
          data.frame(stringsAsFactors = F) %>%
          setNames(gsub("X.", "@", names(.))) %>%
          tibble()}
        available_dataset = available_dataset[1:5] %>%
          data.frame(stringsAsFactors = F) %>%
          setNames(gsub("X.", "@", names(.))) %>%
          mutate(Obs = list(Obs_data))
      }
      
      
      single_df = list()
      for (i in 1:nrow(available_dataset)){
        err_df <- try(df <- available_dataset$Obs[i][[1]], silent = T)
        if (typeof(df) == "character" | class(err_df) == "try-error"){
          df = available_dataset$Obs[i, ] %>%
            setNames(c("@TIME_PERIOD", "@OBS_VALUE")) %>%
            # t() %>%
            data.frame(stringsAsFactors = F) %>%
            setNames(gsub("X.", "@", names(.)))
          single_df[[i]] = tibble(df)
        }
        column_check = sum((colnames(df) %in% c("@TIME_PERIOD", "@OBS_VALUE")) == T)
        rows_to_add = rows_to_add %>%
          bind_rows(
            available_dataset[i,] %>%
              mutate(nrow = nrow(df),
                     date_min = min(df$`@TIME_PERIOD`, na.rm = T),
                     date_max = max(df$`@TIME_PERIOD`, na.rm = T),
                     tot_NA = sum(is.na(df$`@OBS_VALUE`)),
                     column_warning_0_ok = ifelse(column_check != 2, column_check, 0))
          )
      } # i
      if (length(single_df) > 0){
        rows_to_add = rows_to_add %>%
          select(-starts_with("Obs")) %>%
          mutate(Obs = single_df)
      }
      
      if (ctry == "NA"){
        rows_to_add$`@REF_AREA` = "NA"
      }
      
      rows_to_add = rows_to_add %>%
        mutate(status = "OK")
      
    } else {
      error_row = data.frame(`@INDICATOR` = var,
                             `@REF_AREA` = ctry,
                             status = "SKIPPED", stringsAsFactors = F) %>%
                    setNames(gsub("X.", "@", names(.)))
    }
    
    Sys.sleep(0.6)
    
  } # class(err) == "try-error"
  
  return(list(rows_to_add = rows_to_add,
              error_row = error_row))
}

# evaluates quantiles for different date format
date_quantile = function(dates, quant = 0.5, freq = "A", col_prefix = ""){
  # "freq" can be "A" (2010), "Q" (2010-Q1), "M" (2010-01)
  # "quant" can be also a vector
  
  if (freq == "A"){
    dates = as.numeric(dates)
  } else if (freq == "Q"){
    dates = as.numeric(dates %>% gsub("-Q", "\\.", .))
  } else if (freq == "M"){
    dates = as.numeric(dates %>% gsub("-", "\\.", .))
  }
  
  out_quant = quantile(dates, quant, type = 3)   # closest value
  
  if (freq == "A"){
    out_quant = as.character(out_quant)
  } else if (freq == "Q"){
    out_quant = as.character(out_quant) %>% gsub("\\.", "-Q", .)
  } else if (freq == "M"){
    out_quant = as.character(out_quant) %>% gsub("\\.", "-", .)
  }
  
  out_df = data.frame(t(out_quant)) %>%
    setNames(paste0(col_prefix, round(quant * 100)))
  
  return(out_df)
}