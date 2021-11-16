

library("jsonlite")
library(httr)




con = "AD+AE+AF+AG+AI+AL+AM+AN+AO+AQ+AR+AS+AT+AU+AW+AX+AZ+BA+BB+BD+BE+BF+BG+BH+BI+BJ+BL+BM+BN+BO+BQ+BR+BS+BT+BV+BW+BY+BZ+CA+CC+CD+CF+CG+CH+CI+CK+CL+CM+CN+CO+CR+CS+CU+CV+CW+CX+CY+CZ+DD+DE+DJ+DK+DM+DO+DZ+EC+EE+EG+EH+ER+ES+ET+FI+FJ+FK+FM+FO+FR+GA+GB+GD+GE+GF+GG+GH+GI+GL+GM+GN+GP+GQ+GR+GS+GT+GU+GW+GY+HK+HM+HN+HR+HT+HU+ID+IE+IL+IM+IN+IO+IQ+IR+IS+IT+JE+JM+JO+JP+KE+KG+KH+KI+KM+KN+KP+KR+KW+KY+KZ+LA+LB+LC+LI+LK+LR+LS+LT+LU+LV+LY+MA+MC+MD+ME+MF+MG+MH+MK+ML+MM+MN+MO+MP+MQ+MR+MS+MT+MU+MV+MW+MX+MY+MZ+NA+NC+NE+NF+NG+NI+NL+NO+NP+NR+NU+NZ+OM+PA+PE+PF+PG+PH+PK+PL+PM+PN+PR+PS+PT+PW+PY+QA+RE+RO+RS+RU+RW+SA+SB+SC+SD+SE+SG+SH+SI+SJ+SK+SL+SM+SN+SO+SR+SS+ST+SV+SX+SY+SZ+TC+TD+TF+TG+TH+TJ+TK+TL+TM+TN+TO+TR+TT+TV+TW+TZ+UA+UG+UM+US+UY+UZ+VA+VC+VE+VG+VI+VN+VU+WF+WS+YD+YE+YT+YU+ZA+ZM+ZW"
con_ll = strsplit(con, "\\+")[[1]]


coun_check = country_list %>%
  left_join(data.frame(values = con_ll, match = "YES", stringsAsFactors = F))

hh = coun_check %>%
  filter(is.na(match)) %>%
  pull(values)

xx = df_download %>%
  filter(`@REF_AREA` %in% hh)

hh1 = setdiff(con_ll, country_list$values)



uu = "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/FSI/.VE+VG+VI+VN+VU+WF+WS+YD+YE+YT+YU+ZA+ZM+ZW.FSKA_PT?startPeriod=1900&endPeriod=2021"

uu1 = paste0("http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/FSI/.", paste0(con_ll[1:80], collapse = "+"), ".FSKA_PT?startPeriod=1900&endPeriod=2021")


uu2 = "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/FSI/.VE+VG+VI+VN+VU+WF+WS+YD+YE+YT+YU+ZA+ZM+ZW+XK+1C_473.FSKA_PT?startPeriod=1900&endPeriod=2021"

gg = jsonlite::fromJSON(uu2)

aa = gg$CompactData$DataSet$Series


aa = list()
for (i in 1:30){
  btc <- jsonlite::fromJSON(uu)
  aa[[i]] = btc
  if (i %% 9 == 0){Sys.sleep(1)}
}




prox_ip_and_port <- getProxy(port = "3128",
                             country = "IT",
                             action = "start")

prox_ip_and_port <- getProxy(port = "3128",
                             country = "RU",
                             action = "get")


gg <- httr::content(
  httr::GET(
    uu, 
    httr::use_proxy("212.237.16.60", 3128)
  )
)


aa = list()
for (i in 1:30){
  gg <- httr::content(
    httr::GET(
      uu, 
      httr::use_proxy("212.237.16.60", 3128)
    )
  )
  aa[[i]] = gg
  if (i %% 9 == 0){Sys.sleep(0.1)}
}
