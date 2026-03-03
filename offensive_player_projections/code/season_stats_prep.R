###
# Packages
###

library(tidyverse)
library(ggplot2)

###
# loading season stats
###

#Downloaded Season Stats data from Baseball Savant
batting_season_stats = read_csv("season_stats/bats/2015_2025_bats.csv")

###
# Standardizing the Names in Batting Season Stats
###

batting_season_stats = batting_season_stats %>% 
  mutate(batter_full_name = 
           paste(substring(`last_name, first_name`, 
                           str_locate(`last_name, first_name`,",")[,1] + 2,
                           str_length(`last_name, first_name`))," ",
                 substring(`last_name, first_name`,0,
                           str_locate(`last_name, first_name`,",")[,1] - 1),
                 sep = "")) %>% select(-`last_name, first_name`)

write_csv(batting_season_stats, "season_stats/bats/2015_2025_bats.csv")