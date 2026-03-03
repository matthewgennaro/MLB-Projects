# Functions used throughout proccess

# Required packages
library(httr)
library(jsonlite)
library(tidyverse)
library(ggplot2)
library(baseballr)
library(snakecase)

# Pulling condensed list of mlb game information over set time frame
mlb_games = function(season_start, season_end){
  
  seasons = season_start:season_end
  
  #raw schedule pull
  raw_schedule = map_dfr(seasons, function(s) {
    
    message("Pulling season: ", s)
    
    mlb_schedule(
      season    = s,
      level_ids = 1
    ) %>%
      filter(series_description == "Regular Season")
  })
  
  #reducing and cleaning columns names from raw schedule
  
  mlb_games = raw_schedule %>% transmute(
    game_pk               = game_pk,
    season                = season,
    official_date         = as.Date(official_date),
    game_number           = game_number,
    double_header         = double_header,
    day_night             = day_night,
    games_in_series       = games_in_series,
    series_game_number    = series_game_number,
    series_description    = series_description,
    
    away_score            = teams_away_score,
    away_team_id          = teams_away_team_id,
    away_team_name        = teams_away_team_name,
    away_wins             = teams_away_league_record_wins,
    away_losses           = teams_away_league_record_losses,
    
    home_score            = teams_home_score,
    home_team_id          = teams_home_team_id,
    home_team_name        = teams_home_team_name,
    home_wins             = teams_home_league_record_wins,
    home_losses           = teams_home_league_record_losses,
    
    venue_id              = venue_id,
    venue_name            = venue_name)
  
  return(mlb_games)
}

# Pulling pitch level data from mlb stats api
# longest (time required) function as pulling data from each individual game
# setting wd directory pathway to where you would like the data stored
# determine if you want to keep the individual game and individual season files

pitch_level = function(season_start, season_end, mlb_games_df, data_storage_wd,
                       keep_indiviual_game = FALSE, 
                       keep_indiviual_season = FALSE){
  #storing individual games first, then creating season data rds
  #reducing memory usage on computer, temporaily increasing storage usage on
  #removes risk of stoppage via memory limit
  individual_dates_dir = file.path(data_storage_wd, "individual_dates")
  full_seasons_dir = file.path(data_storage_wd, "full_seasons")
  
  dir.create(individual_dates_dir, showWarnings = FALSE)
  dir.create(full_seasons_dir, showWarnings = FALSE)
  
  # Track files that failed
  skipped_files = tibble(season = integer(),file = character())
  
  seasons = season_start:season_end
  
  for(s in seasons){
    
    season_start_time = Sys.time()
    message("===== Starting season: ", s, " at ", season_start_time, " =====")
    
    # Season folder for individual dates
    season_folder = file.path(individual_dates_dir, as.character(s))
    dir.create(season_folder, showWarnings = FALSE, recursive = TRUE)
    
    # Get all unique game dates for this season
    season_dates = mlb_games %>% filter(season == s) %>% 
      filter(series_description == "Regular Season") %>% pull(official_date) %>% 
      unique() %>% sort()
    
    # -----------------------------
    # Loop through each game date
    # -----------------------------
    for (d in season_dates) {
      
      date_str = format(as.Date(d), "%Y-%m-%d")
      
      rds_file = file.path(season_folder,paste0("mlb_pitch_", date_str, ".rds"))
      
      # Skip if either RDS or CSV already exists
      if (file.exists(rds_file)) {
        message("  Skipping already pulled date: ", d)
        next
      }
      
      message("Pulling games for date: ", as.Date(d))
      
      # Get all game_pks for this date
      game_pks = mlb_games %>% filter(season == s, official_date == d) %>%
        pull(game_pk) %>% unique()
      
      if (length(game_pks) == 0) next
      
      # Pull pitch-level data for each game and combine
      date_pbp = purrr::map_dfr(game_pks, function(pk) {
        
        message("Game: ", pk)
        
        pbp = tryCatch(
          get_pbp_mlb(pk),
          error = function(e) {
            message("Failed game_pk: ", pk)
            return(NULL)
          })
        
        if (!is.null(pbp)) pbp$season = s
        
        # Rate-limit API requests
        Sys.sleep(runif(1, 0.2, 1))
        
        pbp
      })
      
      message("DEBUG: About to save file: ", rds_file)
      message("DEBUG: nrow(date_pbp) = ", nrow(date_pbp))
      
      # Save RDS for this date
      saveRDS(date_pbp, rds_file)
      message("DEBUG: saveRDS completed")
      
      # Clean memory
      rm(date_pbp)
      gc()
    }
    # -----------------------------
    # COMBINE SEASON
    # -----------------------------
    message("===== Combining season: ", s, " =====")
    
    season_files = list.files(season_folder,pattern = "\\.rds$",
                              full.names = TRUE)
    
    if (length(season_files) == 0) {
      message("No files for season ", s)
      next
    }
    
    season_data_list = purrr::map(season_files, function(f) {
      tryCatch(readRDS(f), error = function(e) {
        skipped_files <<- bind_rows(
          skipped_files,
          tibble(season = s, file = basename(f))
        )
        NULL
      })
    }) %>% purrr::compact()
    
    all_cols = season_data_list %>% map(names) %>% unlist() %>% unique()
    
    season_data_list = map(season_data_list, function(df) {
      df[setdiff(all_cols, names(df))] = NA_character_
      df %>% mutate(across(all_of(all_cols), as.character))
    })
    
    season_data = bind_rows(season_data_list)
    
    season_out = file.path(full_seasons_dir,
                           paste0("mlb_pitch_data_", s, ".csv"))
    
    write_csv(season_data, season_out)
    message("Saved season file: ", season_out)
    
    rm(season_data, season_data_list)
    gc()
    
    # ---- DELETE DAILY FILES ----
    if (!keep_individual_game) {
      unlink(season_folder, recursive = TRUE)
      message("Deleted individual game files for season ", s)
    }
    
    message("===== Finished season: ", s, " =====\n")
  }
  
  message("===== Combining all seasons =====")
  
  season_csvs = list.files(full_seasons_dir,
                           pattern = "mlb_pitch_data_\\d+\\.csv$",
                           full.names = TRUE)
  
  full_data = purrr::map_dfr(season_csvs, read_csv, show_col_types = FALSE)
  
  full_out = file.path(full_seasons_dir, "mlb_pitch_data_all_seasons.csv")
  write_csv(full_data, full_out)
  
  message("Saved full dataset: ", full_out)
  message("Rows: ", nrow(full_data))
  
  # ---- DELETE SEASON FILES ----
  if (!keep_individual_season) {
    file.remove(season_csvs)
    message("Deleted individual season files")
  }
  
  # -----------------------------
  # SAVE SKIPPED FILE LOG
  # -----------------------------
  skipped_file_log = file.path(full_seasons_dir, "skipped_files.csv")
  write_csv(skipped_files, skipped_file_log)
  
  message("Skipped files logged: ", skipped_file_log)
}

# Cleaning statcast data from mlb stats api pull

clean_baseball_mlb = function(df){
  
  #dropping wanted columns
  unwanted = c("reviewDetails.isOverturned.x", "reviewDetails.inProgress.x",
               "reviewDetails.reviewType.x","reviewDetails.isOverturned.y",
               "reviewDetails.inProgress.y","reviewDetails.reviewType.y",           
               "reviewDetails.challengeTeamId.x",
               "reviewDetails.challengeTeamId.y",
               "reviewDetails.additionalReviews","matchup.splits.batter",
               "matchup.splits.pitcher", "matchup.pitchHand.description",
               "matchup.batSide.description","away_level_id",
               "home_level_id", "home_level_name", "home_parentOrg_id",
               "home_parentOrg_name","home_league_id","home_league_name",
               "away_level_name","away_parentOrg_id","away_parentOrg_name",
               "away_league_id", "away_league_name" )
  
  df = df %>% select(-any_of(unwanted))
  
  #changing columns names
  
  #first applying snake case
  names(df) = to_snake_case(names(df))
  
  #then removing unneeded naming conventions
  #i.e. hit_data to hit
  df = df %>% rename_with(~gsub("reviewDetails", "review",.),
                          cols = matches("reviewDetails")) %>%
    rename_with(~gsub("hit_data", "hit",.), cols = matches("hit_data")) %>%
    rename_with(~gsub("pitch_data_breaks", "pitch", .), 
                cols = matches("pitch_data_breaks")) %>%
    rename_with(~gsub("pitch_data", "pitch",.), 
                cols = matches("pitch_data")) %>%
    rename_with(~gsub("matchup_","",.), cols = matches("matchup_")) %>%
    rename_with(~gsub("about", "ab",.), cols = matches("about"))
  
  return(df)
}

# Pulling Stadium Information from Baseball Savant

ballpark_information = function(season_start, season_end) {
  
  require(dplyr)
  require(stringr)
  require(jsonlite)
  require(httr)
  require(janitor)
  
  seasons = season_start:season_end
  
  #fetch park dimensions
  fetch_park_dimensions = function(year) {
    
    url = paste0(
      "https://baseballsavant.mlb.com/leaderboard/statcast-park-factors?",
      "type=dimensions&year=", year,
      "&batSide=&stat=index_hardhit&condition=All",
      "&rolling=3&parks=mlb&fenceStatType=distance"
    )
    
    res  = httr::GET(url)
    html = httr::content(res, "text", encoding = "UTF-8")
    
    json_text = str_extract(html, "data\\s*=\\s*\\[.*?\\];") %>%
      str_remove("data\\s*=\\s*") %>%
      str_remove(";$")
    
    df = jsonlite::fromJSON(json_text, flatten = TRUE) %>%
      as.data.frame(stringsAsFactors = FALSE)
    
    df$year = year
    df
  }
  
  park_dim_raw = purrr::map_dfr(seasons, fetch_park_dimensions)
  
  park_dim_clean = park_dim_raw %>%
    select(-any_of("rank")) %>%
    rename_with(~ .x %>%
                  str_remove_all("\\(feet\\)|\\(sq\\. feet\\)") %>%
                  str_trim() %>%
                  janitor::make_clean_names())
  

  #fetch park distance metrics
  fetch_park_distance = function(year) {
    
    url = paste0(
      "https://baseballsavant.mlb.com/leaderboard/statcast-park-factors?",
      "type=distance&year=", year,
      "&batSide=&stat=index_hardhit&condition=All",
      "&rolling=3&parks=mlb"
    )
    
    res  = httr::GET(url)
    html = httr::content(res, "text", encoding = "UTF-8")
    
    json_text = str_extract(html, "data\\s*=\\s*\\[.*?\\];") %>%
      str_remove("data\\s*=\\s*") %>%
      str_remove(";$")
    
    df = jsonlite::fromJSON(json_text, flatten = TRUE) %>%
      as.data.frame(stringsAsFactors = FALSE)
    
    df$year = year
    df
  }
  
  park_dist_raw = purrr::map_dfr(seasons, fetch_park_distance)
  
  park_dist_clean = park_dist_raw %>% select(-any_of(c("rk", "rank"))) %>%
    rename_with(~ .x %>%
                  str_remove_all("\\(feet\\)|\\(sq\\. feet\\)|\\(\\%\\)") %>%
                  str_trim() %>% janitor::make_clean_names()) %>%
    mutate(venue_id = as.integer(venue_id), 
           main_team_id = as.integer(main_team_id))
  
  #merge datasets
  parks_merged = park_dist_clean %>% left_join(park_dim_clean,
      by = c("venue_id", "year"), suffix = c("_distance", "_dimensions"))
  
  cols_to_drop = c("distance_lf_line_diff", "distance_lf_gap_diff",
                   "distance_cf_diff", "distance_rf_gap_diff",
                   "distance_rf_line_diff", "distance_deepest_diff",
                   "height_lf_line_diff", "height_lf_gap_diff", "height_cf_diff",
                   "height_rf_gap_diff", "height_rf_line_diff",
                   "height_highest_diff", "avg_fence_distance_diff",
                   "avg_hr_distance_diff", "fence_area_diff",
                   "avg_fence_height_diff")
  
  parks_merged = parks_merged %>% select(-any_of(cols_to_drop))
  
  return(parks_merged)
}

#function to pull the data from the mlb stats api
get_boxscore_safe = function(game_pk) {
  url = paste0("https://statsapi.mlb.com/api/v1/game/", game_pk, "/boxscore")
  
  res = tryCatch(
    httr::GET(url, timeout(10)),
    error = function(e) NULL
  )
  
  if (is.null(res) || res$status_code != 200) return(NULL)
  
  content(res, as = "text", encoding = "UTF-8") %>%
    fromJSON(flatten = TRUE)
}

#extracting weather data

  
#extracting umpire data

#player interval splits
time_interval_sprint = function(year){
  #url
  url = paste0(
    "https://baseballsavant.mlb.com/running_splits?type=raw&bats=&year=",
    year, "&position=&team=&min=0")
  
  # request the raw HTML
  res = httr::GET(url)
  html = httr::content(res, "text", encoding = "UTF-8")
  
  # extract the JSON data embedded in JS
  json_text = str_extract(html, "data\\s*=\\s*\\[.*?\\];")
  json_text = str_remove(json_text, "data\\s*=\\s*")
  json_text = str_remove(json_text, ";$")
  
  # parse JSON → tibble
  df = fromJSON(json_text, flatten = TRUE) %>% 
    as.data.frame(stringsAsFactors = FALSE) %>% .[, 2:36]
  
  #removing unwanted variables
  df = df %>% select(-batter_started_at_position, -name_display_club,
                     -file_code, -name_abbrev, -game_pk, -play_id, -n, 
                     -seconds_decoil)
  
  return(df)
  
}

#player sprint speed
player_sprint_speed = function(year){
  #url
  url = paste0(
    "https://baseballsavant.mlb.com/leaderboard/sprint_speed?min_season=",
    year,"&max_season=",year,"&position=&team=&min=0&sort=0&sortDir=desc")
  
  # request the raw HTML
  res = httr::GET(url)
  html = httr::content(res, "text", encoding = "UTF-8")
  
  # extract the JSON data embedded in JS
  json_text = str_extract(html, "data\\s*=\\s*\\[.*?\\];")
  json_text = str_remove(json_text, "data\\s*=\\s*")
  json_text = str_remove(json_text, ";$")
  
  # parse JSON → tibble
  df = fromJSON(json_text, flatten = TRUE) %>% 
    as.data.frame(stringsAsFactors = FALSE) %>% .[, c(1, 3, 6, 11, 13, 19)]
  
  #add year
  df$year = year
  
  return(df)
}