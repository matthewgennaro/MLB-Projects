# Measuring Player Acceleration and Combining with Top Speed

###
# Packages
###

library(tidyverse)
library(ggplot2)

###
# Utility Functions
###

source("player_speed_utility_functions.R")

###
# Read in Player speed data from Baseball Savant
###

player_dist_interval = read_csv("extra_data/player_split_times.csv")
player_speed = read_csv("extra_data/player_sprint_avg.csv")

###
# Merge the two dataframes together
###

#some na since Baseball Savant doesnt have time splits for all players
#from 2015-2025
player_dist_interval = player_speed %>% 
  left_join(player_dist_interval, by = c("batter_full_name", 
                                         "runner_id" = "player_id","age", 
                                         "team_id", "year"))

remove(player_speed)
###
# Calculating Split Times
###

#selecting time columns names for mutate
dist_cols = names(player_dist_interval) %>%
  str_subset("^seconds_since_hit_") %>%
  sort()

player_dist_interval = create_splits(player_dist_interval, dist_cols)

###
# Calculating Velocity
###

#selecting split columns names for mutate
split_cols = names(player_dist_interval) %>%
  str_subset("^split_")

player_dist_interval = create_velocity(player_dist_interval, split_cols)

###
# Calculating Acceleration
###

#selecting velcoity columns for mutate
vel_cols = names(player_dist_interval) %>%
  str_subset("^vel_split_") %>%
  sort()

player_dist_interval = create_acceleration(player_dist_interval, vel_cols)

### 
# Time to Peak Velocity
###

# velocity columns (already created)
vel_matrix = as.matrix(player_dist_interval[, vel_cols])

# cumulative time columns aligned with velocity columns
# remove the 000 column so positions match
time_matrix = as.matrix(player_dist_interval[, dist_cols[-1]])

# find index of peak velocity per row
# ties for max peak velocity go to the first instance
peak_index = max.col(vel_matrix, ties.method = "first")

# extract matching cumulative time
player_dist_interval$time_to_peak_velocity =
  time_matrix[cbind(seq_len(nrow(time_matrix)), peak_index)]

###
# Peak Acceleration
###
acc_cols = names(player_dist_interval) %>%
  str_subset("^acc_")

player_dist_interval$peak_acceleration =
  apply(player_dist_interval[, acc_cols], 1, max, na.rm = TRUE)


#for players with missing time interval data, peak acceleration is calcuating
#as -Inf instead of NA, so manually encoding those
player_dist_interval = player_dist_interval %>% 
  mutate(peak_acceleration = ifelse(peak_acceleration == "-Inf", NA, 
                                    peak_acceleration))

###
# Fixing Issue with Peak Acceleration 
# core of issue found in time interval data
###

#seems to be data issue as max peak_acceleration is 88 when average is 23
#only 2 instances above 40 each being above 80
#in players other reasons, common consistence in acceleration and 
#time interval pattern
#mean(player_dist_interval$peak_acceleration, na.rm = TRUE) - 23.37617
#max(player_dist_interval$peak_acceleration, na.rm = TRUE) - 88.887

#creating rule where peak acceleration is > 40 then the time splits that
#cause the irregular/impossible acceleration amount will be replaced
#with the average time splits for the player
threshold = 39.999

#grabbing rows with issues in acceleration
rows_to_fix = player_dist_interval %>%
  mutate(peak_acc = do.call(pmax, c(across(all_of(acc_cols)), na.rm = TRUE)))%>%
  filter(peak_acc > threshold)

#finding the irregular time split
player_dist_interval = player_dist_interval %>% rowwise() %>% 
  mutate(peak_acc_col = 
           if (!is.na(peak_acceleration) && peak_acceleration > threshold) {
             acc_cols[which.max(c_across(all_of(acc_cols)))]
             } else {NA_character_}) %>% ungroup()

#pulling out the feet for that it occured in
player_dist_interval = player_dist_interval %>%
  mutate(problem_distance = 
           ifelse(!is.na(peak_acc_col),
                  paste0("0",as.character(as.integer(
                    str_extract(peak_acc_col, "\\d+"))-5)),
                  NA_character_))

#replacing the likely incorrect time split with the mean across other seasons
for (i in seq_len(nrow(player_dist_interval))) {
  
  if (!is.na(player_dist_interval$problem_distance[i])) {
    
    dist_num = as.integer(player_dist_interval$problem_distance[i])
    player = player_dist_interval$player_id[i]
    season_val = player_dist_interval$year[i]
    
    time_col = paste0("seconds_since_hit_", dist_num)
    
    prev_col = paste0("seconds_since_hit_", sprintf("%03d", dist_num - 5))
    next_col = paste0("seconds_since_hit_", sprintf("%03d", dist_num + 5))
    cur_col  = paste0("seconds_since_hit_", sprintf("%03d", dist_num))
    
    prev_time = player_dist_interval[i, prev_col]
    next_time = player_dist_interval[i, next_col]
    
    replacement_value = prev_time + (next_time - prev_time) / 2
    
    player_dist_interval[i, cur_col] = replacement_value
  }
}  

#removing existing split, velocity, and acceleration columns
player_dist_interval = player_dist_interval %>%
  select(-all_of(split_cols), -all_of(vel_cols), -all_of(acc_cols))

#recreating them
player_dist_interval = create_splits(player_dist_interval, dist_cols)
player_dist_interval = create_velocity(player_dist_interval, split_cols)
player_dist_interval = create_acceleration(player_dist_interval, vel_cols)

#recalculating peak acceleration and time to peak velocity
vel_matrix  = as.matrix(player_dist_interval[, vel_cols])
time_matrix = as.matrix(player_dist_interval[, dist_cols[-1]])

peak_index = max.col(vel_matrix, ties.method = "first")

player_dist_interval$time_to_peak_velocity =
  time_matrix[cbind(seq_len(nrow(time_matrix)), peak_index)]


player_dist_interval$peak_acceleration =
  do.call(pmax, c(player_dist_interval[, acc_cols], na.rm = TRUE))

#for players with missing time interval data, peak acceleration is calculating
#as -Inf instead of NA, so manually encoding those
player_dist_interval = player_dist_interval %>% 
  mutate(peak_acceleration = ifelse(peak_acceleration == "-Inf", NA, 
                                    peak_acceleration))

remove(threshold)
remove(time_col)
remove(split_cols)
remove(acc_cols)
remove(cur_col)
remove(dist_cols)
remove(dist_num)
remove(i)
remove(next_col)
remove(player)
remove(vel_cols)
remove(season_val)
remove(vel_matrix)
remove(time_matrix)
remove(peak_index)
remove(next_time)
remove(prev_time)
remove(replacement_value)
remove(rows_to_fix)
remove(prev_col)

###
# Early Acceleration Average (0 - 20 ft)
###
#early_acc_cols = c("acc_seconds_since_hit_005","acc_seconds_since_hit_010",
#                   "acc_seconds_since_hit_015","acc_seconds_since_hit_020")

#player_dist_interval$early_acc_avg =
#  rowMeans(player_dist_interval[, early_acc_cols], na.rm = TRUE)

#checking the correlations
#cor(player_dist_interval %>% 
#      select(peak_acceleration,time_to_peak_velocity,top_sprint_speed_avg) %>% 
#      drop_na())

###
# Using Models Predict time_to_peak_velocity, peak_accleration, and accel_rating
# needed for players without time interval data
# using position_name and top_sprint_speed_avg
###

#viewing relationship between top_sprint_speed_avg and position with 
#time_to_peak_velocity and peak_acceleration
#ggplot(player_dist_interval, 
#       aes(x = top_sprint_speed_avg, y = time_to_peak_velocity, 
#           colour = position_name)) + geom_point()

#ggplot(player_dist_interval, 
#       aes(x = top_sprint_speed_avg, y = peak_acceleration, 
#           colour = position_name)) + geom_point()

#linear model to predict time_to_peak_velocity
peak_velo_model = lm(time_to_peak_velocity ~ position_name + 
                       top_sprint_speed_avg, data = player_dist_interval, 
                     na.action = na.exclude)

#summary stats of model
#summary(peak_velo_model) # model is very weak, not that surprised



#grabbing missing rows
missing_rows = which(is.na(player_dist_interval$time_to_peak_velocity))

#inserting predicted time_to_peak_velocity for NAs
player_dist_interval$time_to_peak_velocity[missing_rows] =
  predict(peak_velo_model, newdata = player_dist_interval[missing_rows, ])

#linear model to predict peak_acceleration
peak_accel_model = lm(peak_acceleration ~ position_name +
                        top_sprint_speed_avg, data = player_dist_interval, 
                      na.action = na.exclude)

#summary stats of model
#summary(peak_accel_model) #model performs alright

#grabbing missing rows
missing_rows = which(is.na(player_dist_interval$peak_acceleration))

#inserting predicted peak_acceleration for NAs
player_dist_interval$peak_acceleration[missing_rows] <-
  predict(peak_accel_model, newdata = player_dist_interval[missing_rows, ])

remove(peak_velo_model)
remove(peak_accel_model)
remove(missing_rows)

###
# Calculating Acceleration Rating
# scale of 0-100 percentile
# using z-scores
###

#calculating average peak acceleration and time to peak velocity
avg_peak_accel = mean(player_dist_interval$peak_acceleration, na.rm = TRUE)
avg_time_velo = mean(player_dist_interval$time_to_peak_velocity, na.rm = TRUE)

#calculating sd peak acceleration and time to peak velocity
sd_peak_accel = sd(player_dist_interval$peak_acceleration, na.rm = TRUE)
sd_time_velo = sd(player_dist_interval$time_to_peak_velocity, na.rm = TRUE)

#computing z_scores and combined rating on 35/65 importance split
#40 will be applied to peak accel since the data seems more unreliable
#than time to velocity
#note: inverse will be applied to time to peak velocity
#since faster times are better, slow times are worse
player_dist_interval = player_dist_interval %>%
  mutate(z_peak_accel = (peak_acceleration - avg_peak_accel)/sd_peak_accel,
         z_time_velo = -(time_to_peak_velocity - avg_time_velo)/sd_time_velo,
         combined_z = 0.35*z_peak_accel + 0.65*z_time_velo)

remove(avg_peak_accel)
remove(avg_time_velo)

remove(sd_peak_accel)
remove(sd_time_velo)

#calculating min and max scores
min_combined = min(player_dist_interval$combined_z, na.rm = TRUE)
max_combined = max(player_dist_interval$combined_z, na.rm = TRUE)

#re-scaling combined z to a scale of 0-100
player_dist_interval = player_dist_interval %>% 
  mutate(accel_rating = 
           100 * (combined_z - min_combined)/(max_combined - min_combined))

#keep only select columns
player_dist_interval = player_dist_interval %>%
  select(runner_id, year, top_sprint_speed_avg,
         time_to_peak_velocity, peak_acceleration, accel_rating, position_name,
         bat_side)

remove(min_combined)
remove(max_combined)