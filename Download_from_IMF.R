

library(lubridate)
library(imfr)
library(data.table)
library(dplyr)
library(tidyverse)
library(stringr)




########## dataset list

dataset_list = imf_ids(return_raw = T, times = 3)$Structure$Dataflows$Dataflow




########## get list of code for selected dataset

database_id = "FSI"  # from KeyFamilyRef$KeyFamilyID

df_codelist = imf_codelist(database_id, return_raw = FALSE, times = 3) %>%
  mutate_all(as.character)




########## get codes from codelist

df_codes = c()
for(i in 1:nrow(df_codelist)){
  df_codes = df_codes %>%
    bind_rows(imf_codes(df_codelist$codelist[i], return_raw = FALSE, times = 3) %>%
                mutate_all(as.character) %>%
                rename(values = codes) %>%
                mutate(code = df_codelist$codelist[i],
                       code_description = df_codelist$description[i])) %>%
    select(code, code_description, values, description)
}



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

df_download = c()

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
      
      
      
      suppressWarnings(rm(down_data, available_dataset, err))
      
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
        
        error_list = error_list %>%
          bind_rows(data.frame(`@INDICATOR` = var,
                               `@REF_AREA` = ctry,
                               status = "DOWNLOAD_FAILED", stringsAsFactors = F) %>%
                      setNames(gsub("X.", "@", names(.))))
        
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
          
          rows_to_add = c()
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
          
          df_download = df_download %>%
            bind_rows(rows_to_add %>%
                        mutate(status = "OK"))
          
        } else {
          error_list = error_list %>%
            bind_rows(data.frame(`@INDICATOR` = var,
                                 `@REF_AREA` = ctry,
                                 status = "SKIPPED", stringsAsFactors = F) %>%
                        setNames(gsub("X.", "@", names(.))))
        }
        
        
        Sys.sleep(0.6)
        
      } # class(err) == "try-error"
      
      
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

df_final = df_download %>%
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

saveRDS(df_final %>% bind_rows(error_list), './df_final.rds')

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

# summary by indicator

summary_indicator = df_final %>%
  filter(status == "OK") %>%
  group_by(indicator_main, indicator, indicator_description, freq) %>%
  summarize(total_country = uniqueN(country_code),
            min_date = min(date_min),
            min_date_25 = quantile(date_min, 0.25),
            min_date_50 = quantile(date_min, 0.5),
            min_date_75 = quantile(date_min, 0.75),
            min_date_MAX = max(date_min),
            tt = n(), .groups = "drop") %>%
  mutate(dd = total_country - tt)


aa = readRDS('./df_download.rds')


date_quantile = function(dates, quant = 0.5, freq = "A"){
  # "freq" can be "A" (2010), "Q" (2010-Q1), "M" (2010-01)
  # "quant" can be also a vector
  
  if (freq == "A"){
    dates = as.numeric(dates)
  } else if (freq == "Q"){
    dates = as.numeric(dates %>% gsub("-Q", "\\.", .))
  } else if (freq == "M"){
    dates = as.numeric(dates %>% gsub("-", "\\.", .))
  }
  
  out_quant = quantile(dates, quant)
  
  if (freq == "A"){
    out_quant = as.character(out_quant)
  } else if (freq == "Q"){
    out_quant = as.character(out_quant) %>% gsub("\\.", "-Q", .)
  } else if (freq == "M"){
    out_quant = as.character(out_quant) %>% gsub("\\.", "-", .)
  }
  
  return(out_quant)
}

ff = "A"
df_final %>%
  filter(freq == ff) %>%
  pull(date_min) %>%
  date_quantile(., c(0.2, 0.4), ff)



ind = "FSKA_PT"
vv=imf_data(
  database_id,
  ind,
  country = "IT",
  start = 1900,
  end = current_year(),
  freq = "A",
  return_raw = T,
  print_url = T,
  times = 3
)
aa=vv$CompactData$DataSet$Series
vv$iso2c %>% uniqueN()






aa = df_download %>% filter(`@REF_AREA` %in% country_list$values)
