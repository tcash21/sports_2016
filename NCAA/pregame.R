library(googlesheets)
library(randomForest)
library(data.table)
library(RSQLite)
library(dplyr)

drv <- dbDriver("SQLite")
con <- dbConnect(drv, "/home/ec2-user/sports2016/NCAA/sports.db")

tables <- dbListTables(con)

lDataFrames <- vector("list", length=length(tables))

## create a data.frame for each table
for (i in seq(along=tables)) {
  if(tables[[i]] == 'NCAASBHalfLines' | tables[[i]] == 'NCAASBLines'){
  lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT away_team, home_team, game_date, line, spread, max(game_time) as
game_time from ", tables[[i]], " group by away_team, home_team, game_date;"))

  } else {
    lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT * FROM '", tables[[i]], "'", sep=""))
  }
  cat(tables[[i]], ":", i, "\n")
}

lines <- lDataFrames[[which(tables == "NCAASBLines")]]
lookup <- lDataFrames[[which(tables == "NCAASBTeamLookup")]]
halfbox <- lDataFrames[[which(tables == "NCAAstats")]]
finalbox <- lDataFrames[[which(tables == "NCAAfinalstats")]]
games <- lDataFrames[[which(tables == "NCAAgames")]]

## Add ESPN abbreviations to line data
lines$away_espn<-lookup[match(lines$away_team, lookup$sb_team),]$espn_abbr
lines$home_espn<-lookup[match(lines$home_team, lookup$sb_team),]$espn_abbr
lines <- lines[c("away_espn", "home_espn", "line", "spread", "game_date")]

## Join games and half lines data
#games$game_time <- games$game_date
games$game_date<-substr(games$game_date,0,10)
games$key <- paste(games$team1, games$team2, games$game_date)

lines$key <- paste(lines$away_espn, lines$home_espn, lines$game_date)
games <- merge(lines, games[c("game_id", "key", "game_time")], by="key")

lines <- games
#lines$game_time<-as.POSIXct(lines$game_time, format='%m/%d/%Y %H:%M')
lines$game_time <- as.POSIXct(paste(lines$game_date, lines$game_time), format='%m/%d/%Y %H:%M')
lines <- lines[order(lines$game_time, decreasing=TRUE),]
lines <- subset(lines, select = -c(key))
lines$spread <- as.numeric(gsub("\\+", "", lines$spread)) 

library(dplyr)

lines <- lines[c("game_id", "game_time", "away_espn", "home_espn", "line", "spread", "game_date")]

gs_auth(token = '/home/ec2-user/sports2016/NCF/ttt.rds')

ncf <- gs_key('1D_uKs4UuAkM4rijIphX-0x2gdqT4wgsXCdC1nTLnMMA', visibility = 'private')

gs_edit_cells(ncf, ws='pregame', input=colnames(lines), byrow=TRUE, anchor="A1")
lines$game_date <- as.Date(lines$game_date, '%m/%d/%Y')
lines <- lines[lines$game_date >= format(Sys.Date()-3, "%Y-%m-%d"),]

gs_edit_cells(ncf, ws='pregame', input = lines, anchor="A2", col_names=FALSE, trim=TRUE)
