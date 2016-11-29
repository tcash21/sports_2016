library(googlesheets)
library(randomForest)
library(data.table)
library(RSQLite)
library(dplyr)

setwd("/home/ec2-user/sports2016/NCF")

drv <- dbDriver("SQLite")
con <- dbConnect(drv, "/home/ec2-user/sports2016/NCF/sports.db")

tables <- dbListTables(con)

lDataFrames <- vector("list", length=length(tables))

## create a data.frame for each table
for (i in seq(along=tables)) {
  if(tables[[i]] == 'NCFSBHalfLines' | tables[[i]] == 'NCFSBLines'){
  lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT away_team, home_team, game_date, line, spread, max(game_time) as
game_time from ", tables[[i]], " group by away_team, home_team, game_date;"))

  } else {
    lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT * FROM '", tables[[i]], "'", sep=""))
  }
  cat(tables[[i]], ":", i, "\n")
}

#halflines <- lDataFrames[[which(tables == "NCFSBHalfLines")]]
lines <- lDataFrames[[which(tables == "NCFSBLines")]]
lookup <- lDataFrames[[which(tables == "NCFSBTeamLookup")]]
halfbox <- lDataFrames[[which(tables == "halfBoxScore")]]
finalbox <- lDataFrames[[which(tables == "finalBoxScore")]]
games <- lDataFrames[[which(tables == "games")]]

## Add ESPN abbreviations to line data
#halflines$away_espn<-lookup[match(halflines$away_team, lookup$sb_team),]$espn_abbr
#halflines$home_espn<-lookup[match(halflines$home_team, lookup$sb_team),]$espn_abbr
lines$away_espn<-lookup[match(lines$away_team, lookup$sb_team),]$espn_abbr
lines$home_espn<-lookup[match(lines$home_team, lookup$sb_team),]$espn_abbr
#halflines <- halflines[c("away_espn", "home_espn", "line", "game_date")]
lines <- lines[c("away_espn", "home_espn", "line", "spread", "game_date")]
#lines$game_date <- as.Date(lines$game_date, format='%m/%d/%Y')
#lines <- lines[order(lines$game_date, desc=TRUE),]

## Join games and half lines data
games$game_time <- games$game_date
games$game_date<-substr(games$game_date,0,10)
games$key <- paste(games$team1, games$team2, games$game_date)
#lines$key <- paste(lines$away_espn, lines$home_espn, lines$game_date)
#games <- merge(lines, games)

lines$key <- paste(lines$away_espn, lines$home_espn, lines$game_date)
games <- merge(lines, games[c("game_id", "key", "game_time")], by="key")

#halfbox <- merge(games, halfbox)[c("game_id", "game_date.x", "away_espn.x", "home_espn.x", "team", "line.x", "spread", "line.y", "first_downs",
#            "third_downs", "fourth_downs", "total_yards", "passing", "comp_att", "yards_per_pass", "rushing", "rushing_attempts",
#            "yards_per_rush","penalties","turnovers", "fumbles_lost","ints_thrown", "possession", "score")]
#halfbox <- halfbox[order(halfbox$game_id),]
#halfbox$tempteam <- ""
#halfbox$tempteam[which(halfbox$team == halfbox$away_espn)] <- "team1"
#halfbox$tempteam[which(halfbox$team != halfbox$away_espn)] <- "team2"

#wide<-reshape(halfbox[,c(-3:-4)], direction = "wide", idvar="game_id", timevar="tempteam")
#wide$first.half.points <- wide$score.team1 + wide$score.team2
#colnames(wide)[6] <- "half.line"
#wide$half.line <- as.numeric(wide$half.line)
#wide$score.diff<-wide$score.team1 - wide$score.team2
#wide$spread.team1 <- as.numeric(gsub("\\+", "", wide$spread.team1)) * -1
#wide$abs.score.diff <- abs(wide$score.diff)
#wide$abs.spread <- abs(wide$spread.team1)
#wide$team1.favorite <- wide$spread.team1 < 0

lines <- games
#lines$game_date <- as.Date(lines$game_date, format='%m/%d/%Y')
lines$game_time<-as.POSIXct(lines$game_time, format='%m/%d/%Y %H:%M')
lines <- lines[order(lines$game_time, decreasing=TRUE),]

#lines <- lines[order(lines$game_date, decreasing=TRUE),]
lines <- subset(lines, select = -c(key))
lines$spread <- as.numeric(gsub("\\+", "", lines$spread)) * -1
#lines$spread <- lines$spread * -1

library(dplyr)

#games$game_time <- as.POSIXct(games$game_time, format='%m/%d/%Y %H:%M')
#games<-games[order(games$game_time, decreasing=TRUE),]
#games<-subset(games, select=c(game_id, game_time, team1, team2, line, spread ))
#games$line <- as.numeric(games$line)
#games$spread<-as.numeric(gsub("\\+", "", games$spread))

lines <- lines[c("game_id", "game_time", "away_espn", "home_espn", "line", "spread", "game_date")]

gs_auth(token = '/home/ec2-user/sports2016/NCF/ttt.rds')

ncf <- gs_key('1W0fTkdFCUNRRdGjXIOOmjeAyJbUUH2o6k0d6JgDcBCQ', visibility = 'private')

gs_edit_cells(ncf, ws='pregame', input=colnames(lines), byrow=TRUE, anchor="A1")

gs_edit_cells(ncf, ws='pregame', input = lines, anchor="A2", col_names=FALSE, trim=TRUE)
