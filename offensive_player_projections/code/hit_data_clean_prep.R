###
# Packages
###

library(tidyverse)
library(ggplot2)

setwd("/Users/me/Desktop/mlb_projects")
###
# Required Source File
###
source("data_pull_functions.R")

###
# Combining Downloaded Pitch Level Data
###
seasons = 2015:2025

#empty df to store every season into
hit_data = data.frame()

#loop to pull each of the select data
for(s in seasons){
  message("===== Downloading season: ", s, " at ", Sys.time(), " =====")
  
  file_name = paste("full_seasons/mlb_pitch_data_", s, ".csv", sep = "")
  
  temp_data = read_csv(file_name)
  
  temp_data = clean_baseball_mlb(temp_data)
  
  #columns to keep
  columns_selected = c("game_pk", "batter_id", "batter_full_name",  
                       "bat_side_code", "at_bat_index", "details_description",
                       "index","pitch_number", "count_balls_start",
                       "count_strikes_start","count_outs_start", "count_balls_end",
                       "count_strikes_end", "count_outs_end",
                       "result_event", "result_event_type", 
                       "pitcher_id", "pitcher_full_name", "pitch_hand_code",
                       "splits_men_on_base", "batting_team", "fielding_team", 
                       "details_type_code","details_type_description", 
                       "pitch_coordinates_p_x", "pitch_coordinates_p_z",
                       "pitch_start_speed", "pitch_end_speed", "pitch_zone",
                       "pitch_break_length", "pitch_break_angle", 
                       "pitch_break_y", "pitch_spin_rate",
                       "pitch_spin_direction", "pitch_break_vertical", 
                       "pitch_break_vertical_induced", "pitch_break_horizontal", 
                       "pitch_strike_zone_top", "pitch_strike_zone_bottom", 
                       "pitch_plate_time","pitch_extension", 
                       "hit_coordinates_coord_x", "hit_coordinates_coord_y",
                       "hit_launch_speed", "hit_launch_angle", "hit_trajectory",
                       "hit_total_distance", "home_team", "season")
  
  #cleaning column names
  temp_data = clean_baseball_mlb(temp_data)
  
  #keeping relevant data
  temp_data = temp_data %>% select(any_of(columns_selected)) %>% 
    mutate(count_start = 
             paste(count_balls_start, "-", count_strikes_start, sep = ""),
           count_end = paste(count_balls_end, "-", count_strikes_end, sep = ""))
  
  #binding it to hit_data
  hit_data = rbind(hit_data, temp_data)
  
  #freeing space
  remove(temp_data)
  
  message("===== Finished season: ", s, " at ", Sys.time(), " =====")
}

###
# Adding home venue to hitting data
###

#temporarily loading teams ref and teams stadium
teams = read_csv("extra_data/teams_ref.csv")
team_stadium_season = read_csv("extra_data/stadium_data.csv")

#adding team ids to teams
hit_data = hit_data %>% left_join(
  (teams %>% mutate(home_team_id = team_id) %>% 
     select(home_team_id, team_name)), 
  by = c("home_team" = "team_name")) %>%
  left_join((teams %>% mutate(batting_team_id = team_id) %>% 
               select(batting_team_id, team_name)), 
            by = c("batting_team" = "team_name")) %>%
  left_join((teams %>% mutate(fielding_team_id = team_id) %>% 
               select(fielding_team_id, team_name)), 
            by = c("fielding_team" = "team_name")) %>%
  mutate(away_team_id = case_when(
    home_team_id == batting_team_id ~ fielding_team_id,
    TRUE ~ batting_team_id))


#pulling select stadium columns
stadium_columns = c("season", "team_id","venue_id", "venue_name_short",
                    "distance_lf_line", "distance_lf_gap", "distance_cf", 
                    "distance_rf_gap", "distance_rf_line", "distance_deepest",
                    "height_lf_line", "height_lf_gap", "height_cf",
                    "height_rf_gap", "height_rf_line","height_highest", 
                    "avg_fence_height", "avg_fence_distance", "avg_temperature", 
                    "avg_roof", "elevation_feet", "fence_area")

hit_data = hit_data %>% 
  left_join((team_stadium_season %>% 
               select(all_of(stadium_columns)) %>% 
               filter(!is.na(venue_name_short))),
            by = c("season", "home_team_id" = "team_id"))

###
# Spray Angle
###
#calculating spray angle
hit_data = hit_data %>% 
  mutate(spray_x = case_when(
    bat_side_code == "R" ~ -(hit_coordinates_coord_x - 127.5),
    bat_side_code == "L" ~ (hit_coordinates_coord_x - 127.5),
    TRUE ~ NA_real_),
    spray_angle = atan2(spray_x, (213-hit_coordinates_coord_y)) * 180/pi,
    spray_bin = case_when(
      is.na(spray_x) ~ NA,
      spray_angle >= 30  ~ "extreme_pull",
      spray_angle >= 15  ~ "pull",
      spray_angle > -15  ~ "center",
      spray_angle > -30  ~ "push",
      TRUE               ~ "extreme_push"
    ))

###
# Filling in Missing Elevation Information
###
hit_data = hit_data %>% group_by(venue_id) %>% mutate(
  elevation_feet = coalesce(elevation_feet, 
                            mean(elevation_feet, na.rm = TRUE))) %>% ungroup()

#writing cleaned data to csv
write_csv(hit_data, "cleaned_hit_data_full.csv")
