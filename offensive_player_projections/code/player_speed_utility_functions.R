###
# Packages
###

library(tidyverse)

###
# Split Time Column Functions
###

create_splits = function(df, dist_cols) {
  
  df = df %>%
    mutate(across(
      .cols = all_of(dist_cols[-1]),  # skip 000
      .fns = ~ . - get(dist_cols[which(dist_cols == cur_column()) - 1]),
      .names = "split_{.col}"))
  
  return(df)
}

###
# Velocity Function
###

create_velocity = function(df, split_cols) {
  
  df = df %>% mutate(across(.cols = all_of(split_cols),
                            .fns  = ~ round(5 / ., 3),
                            .names = "vel_{.col}"))
  
  return(df)
}

###
# Acceleration Function
###

create_acceleration = function(df, vel_cols) {
  
  # Loop through velocity columns (starting at second segment)
  for (i in 2:length(vel_cols)) {
    
    current_vel = vel_cols[i]
    prev_vel    = vel_cols[i - 1]
    
    suffix    = stringr::str_remove(current_vel, "^vel_")
    split_col = suffix
    acc_name  = paste0("acc_", stringr::str_remove(suffix, "^split_"))
    
    df[[acc_name]] =
      round((df[[current_vel]] - df[[prev_vel]]) /df[[split_col]], 3)
  }
  
  # Handle 0 → 5 ft acceleration separately
  first_vel  = vel_cols[1]
  first_split = stringr::str_remove(first_vel, "^vel_")
  
  first_acc_name = paste0("acc_", stringr::str_remove(first_split, "^split_"))
  
  df[[first_acc_name]] = round(df[[first_vel]] / df[[first_split]], 3)
  
  return(df)
}
