

library(lubridate)
library(imfr)
library(data.table)
options(tidyverse.quiet = TRUE)
library(tidyverse)
library(stringr)

source('./Help.R')


###################################   DOWNLOAD DATA   ###################################
# skip if reloading
{
  
  ########## dataset list
  
  dataset_list = imf_ids(return_raw = T, times = 3)$Structure$Dataflows$Dataflow
  
  
  
  
  ########## get list of code for selected dataset
  
  database_id = "FSI"  # from KeyFamilyRef$KeyFamilyID
  
  df_codelist = imf_codelist(database_id, return_raw = FALSE, times = 3) %>%
    mutate_all(as.character)
  
  
  
  
  ########## get codes from codelist
  
  df_codes = fail = c()
  for(i in 1:nrow(df_codelist)){
    err = try(tt <- imf_codes(df_codelist$codelist[i], return_raw = FALSE, times = 3), silent = T)
    
    if (class(err) != "try-error"){
    df_codes = df_codes %>%
      bind_rows(tt %>%
                  mutate_all(as.character) %>%
                  rename(values = codes) %>%
                  mutate(code = df_codelist$codelist[i],
                         code_description = df_codelist$description[i])) %>%
      select(code, code_description, values, description)
    rm(tt)
    } else {
      fail = c(fail, df_codelist$codelist[i])
    }
  }
  if (length(fail) > 0){cat('\n\n- failed to download code:\n', paste0(fail, collapse = '\n'))}
  
  
  
  ########## get metadata structure
  
  # df_meta_structure = imf_metastructure(database_id, return_raw = T)
  
  
  
  ########## get metadata
  
  # ind = "FSRR_PT"
  # vv = imf_metadata(
  #   database_id = database_id,
  #   indicator = ind,
  #   country = "All",
  #   start = 1900,
  #   end = current_year(),
  #   return_raw = F
  # )
  
  
  
  ########## download data
  
  df_download = data.frame()
  
  # reload previous download
  df_download = readRDS('./df_download.rds') %>% unique()
  
  
  country_to_remove = c("1C_977", "1C_Reporters", "1E", "4F", "5B", "5H", "5I", "5M", "5W", "5Y", "7B", "All Countries", "FSD Reporters", "R1", "U2", "X0",
                        "1C_752" ,"1C_355" ,"CSH" , "1C_356" ,"1C_568" ,"TP" ,"1C_473" ,"1C_459" ,"YUC")
  country_list = df_codes %>%
    filter(code_description == "Geographical Areas") %>%
    filter(!values %in% country_to_remove)
  
  variable_to_remove = df_codes %>%
    filter(code_description == "Indicator") %>%
    filter(str_detect(values, "^All"))  %>%       # variables starting with "All"
    pull(values) %>%
    c(., c("ENDE_XDC_USD_RATE", "FSDIndicatorsRatioSmallerthan100Percent", "FSDPositiveIndicators", "GFSRIndicators",
           "I005MetadataSubIndicators", "I015MetadataSubIndicators", "FS2PositiveIndicators"))
  variable_list = df_codes %>%
    filter(code_description == "Indicator") %>%
    filter(!values %in% variable_to_remove) %>%
    mutate(values_main = apply(., 1, function(x) strsplit(x["values"], "_") %>% .[[1]] %>% .[-length(.)] %>% paste0(collapse = "_")))
  
  check_main_values = variable_list %>%
    group_by(values_main) %>%
    summarize(count = n())
  if (max(check_main_values$count) > 3){cat('\n\n##### some values_main with more than 3 values. Check check_main_values')}
  
  
  
  error_list = c()
  reload_check = data.frame()
  tot_country = nrow(country_list)
  tot_variable = nrow(variable_list)
  var_i = overall_count = 1
  start_time = Sys.time()
  for (var in variable_list$values){
    
    
    # var = "FSKA_PT"
    
    if (nrow(df_download) > 0){
      reload_check = df_download %>%
        filter(`@INDICATOR` == var)
    }
    
    if (nrow(reload_check) == 0){
      
      ctry_i = 1
      for (ctry in country_list$values){
        
        
        # var = "FSKA_PT";ctry="DZ"
        
        tot_diff=seconds_to_period(difftime(Sys.time(), start_time, units='secs'))
        # overall_count = (var_i - 1) * tot_variable + ctry_i
        cat('Downloading variable: ', var, '(', var_i, '/', tot_variable, ')  - country:', ctry, '(', ctry_i, '/', tot_country, ')   - ',
            overall_count, '/', tot_country * tot_variable,
            ' elapsed time:', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))),
            '                 ', end = '\r')
        
        DwDt = download_data(database_id, var, ctry)
        
        df_download = df_download %>%
          bind_rows(DwDt$rows_to_add)
        
        error_list = error_list %>%
          bind_rows(DwDt$error_row)
        
        
        ctry_i = ctry_i + 1
        overall_count = overall_count + 1
      } # ctry
      
      saveRDS(df_download %>% bind_rows(error_list), './df_download.rds')
      
    }  else {
      overall_count = overall_count + tot_country
    } # reload_check
    
    var_i = var_i + 1
  } # var
  rm(reload_check)
  tot_diff=seconds_to_period(difftime(Sys.time(), start_time_overall, units='secs'))
  cat('\n\nTotal elapsed time:', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))))

  # attempt to download again failed status
  fail_list = df_download %>%
    filter(status == "DOWNLOAD_FAILED")
  
  if (nrow(fail_list) > 0){
    df_download_ret = error_list_ret = c()
    for (i in 1:nrow(fail_list)){
      
      cat('Re-Downloading variable-country: ', i, '/', nrow(fail_list), end = '\r')
      
      DwDt = download_data(database_id, fail_list$`@INDICATOR`[i], fail_list$`@REF_AREA`[i])
      
      df_download_ret = df_download_ret %>%
        bind_rows(DwDt$rows_to_add)
      
      error_list_ret = error_list_ret %>%
        bind_rows(DwDt$error_row)
    }
    
    df_download = df_download %>%
      filter(status != "DOWNLOAD_FAILED") %>%
      bind_rows(df_download_ret, error_list_ret)
    
    cat('\n\n Recovered pairs:', df_download_ret %>% select(`@REF_AREA`, `@INDICATOR`) %>% uniqueN(), '/', nrow(fail_list))
    if (nrow(error_list_ret %>% filter(status == "DOWNLOAD_FAILED")) > 0){cat('\n  ######  Failed download still present, check error_list_ret')}
    
    saveRDS(df_download, './df_download.rds')
  }
  
  # create final dataframe
  
  df_download = readRDS('./df_download.rds') %>% unique()
  df_final = df_download %>%
    unique() %>%
    setNames(gsub("@", "", names(.))) %>%
    rename(country_code = REF_AREA,
           indicator = INDICATOR,
           freq = FREQ,
           data = Obs) %>%
    # arrange(indicator, country_code) %>%
    left_join(country_list %>%
                select(values, description) %>%
                rename(country_name = description), by = c("country_code" = "values")) %>%
    left_join(variable_list %>%
                select(values, description, values_main) %>%
                rename(indicator_description = description,
                       indicator_main = values_main), by = c("indicator" = "values")) %>%
    left_join(df_codes %>%
                filter(code == "CL_UNIT_MULT") %>%
                select(values, description) %>%
                rename(unit_description = description), by = c("UNIT_MULT" = "values")) %>%
    select(indicator_main, indicator, indicator_description, country_code, country_name, freq, data, nrow, date_min, date_max, tot_NA, column_warning_0_ok, status, everything())
  
  saveRDS(df_final, './df_final.rds')
  
  # double check single-rowed data
  
  index_to_fix = df_final %>%
    mutate(iid = 1:nrow(.)) %>%
    filter(is.na(nrow) & !is.na(date_min)) %>%
    pull(iid)
  
  if (length(index_to_fix) > 0){
    
    tot_na_rows = sum(is.na(df_final$nrow))
    fixed_rows = c()
    for (i in index_to_fix){
      
      row = df_final[i, ]
      if (is.na(row$nrow) & row$status == "OK"){
        
        dd = row$data[[1]]
        vv = dd %>%
          data.frame(stringsAsFactors = F) %>%
          setNames(c("@TIME_PERIOD", "@OBS_VALUE")) %>%
          setNames(gsub("X.", "@", names(.)))
        
        new_list = list()
        new_list[[1]] = vv
        row = row %>%
          select(-data) %>%
          mutate(data = list(vv),
                 nrow = nrow(vv))
        
        fixed_rows = fixed_rows %>%
          bind_rows(row)
      }
    }
    
    df_final = df_final[-index_to_fix, ] %>%
      bind_rows(fixed_rows)
    
    if (tot_na_rows - sum(is.na(df_final$nrow)) != nrow(fixed_rows)){
      cat('\n##### check single-rowed data recovering')
    } else {
      saveRDS(df_final, './df_final.rds')
    }
    
  }
  
  # check downloaded data
  
  check_expected_rows = expand.grid(indicator = variable_list$values, country_code = country_list$values) %>%
    left_join(df_final %>%
                select(indicator, country_code, indicator_description, country_name) %>%
                unique() %>%
                mutate(check = 1), by = c("indicator", "country_code")) %>%
    filter(is.na(check))
  if (nrow(check_expected_rows) > 0){cat("\n\n###### missing combination indicator-country found. Check check_expected_rows")}
  
  check_integrity = df_final %>%
    group_by(indicator_main, country_name, country_code) %>%
    summarize(total_status = uniqueN(status),
              status = paste0(status, collapse = " | "),
              error = sum("DOWNLOAD_FAILED" %in% status), .groups = "drop") %>%
    arrange(desc(error))
  if (sum(check_integrity$error > 0) != 0){cat("\n\n###### error in download. Check check_integrity")}
  
  # report SKIPPED and ERROR
  print(df_final %>%
          group_by(status) %>%
          summarize(Count = n()))
}





###################################   SUMMARY STATISTICS   ###################################

df_final = readRDS('./df_final.rds')


########## summary by indicator

indicator_rename = df_final %>%
  select(indicator_description) %>%
  unique() %>%
  mutate(clean = gsub("Financial Soundness Indicators, ", "", indicator_description)) %>%
  separate(clean, c('indicator_description_main', 'indicator_description_low'), sep= ', ', remove = T, extra = "merge")

summary_indicator = df_final %>%
  filter(status == "OK") %>%
  group_by(indicator_main, indicator, indicator_description, freq) %>%
  summarize(total_country = uniqueN(country_code), .groups = "drop") %>%
  left_join(
    df_final %>%
      filter(status == "OK") %>%
      group_by(indicator_main, indicator, indicator_description, freq) %>%
      do(cbind(., date_quantile(.$date_min, c(0, 0.25, 0.5, 0.75, 1), .$freq, "date_min_"), date_quantile(.$date_max, c(0, 0.25, 0.5, 0.75, 1), .$freq, "date_max_"))) %>%
      select(indicator_main, indicator, indicator_description, freq, starts_with("date_min_"), starts_with("date_max_")) %>%
      unique() %>%
      arrange(indicator_main, indicator, indicator_description, freq) %>%
      ungroup() %>%
      setNames(gsub("_0", "_MIN", names(.))) %>%
      setNames(gsub("_100", "_MAX", names(.)))
  ) %>%
  mutate(date_range_25_75 = paste0(date_min_75, " , ", date_max_25)) %>%
  left_join(indicator_rename, by = "indicator_description") %>%
  select(-indicator_description) %>%
  select(indicator_main, indicator, indicator_description_main, indicator_description_low, freq, total_country, date_range_25_75, everything()) %>%
  arrange(desc(total_country))

write.table(summary_indicator, './downloaded_data_summary.csv', sep = ';', row.names = F, append = F)
















#### back-up checks


ind = "FS_ODX_CC_RES_EUR"
vv=imf_data(
  database_id,
  ind,
  country = "AU",
  start = 1900,
  end = current_year(),
  freq = "A",
  return_raw = T,
  print_url = T,
  times = 3
)
aa=vv$CompactData$DataSet$Series
vv$iso2c %>% uniqueN()



