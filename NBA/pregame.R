library(googlesheets)
library(randomForest)
library(data.table)
library(RSQLite)
library(dplyr)

drv <- dbDriver("SQLite")
con <- dbConnect(drv, "/home/ec2-user/sports2016/NBA/sports.db")

tables <- dbListTables(con)

lDataFrames <- vector("list", length=length(tables))

## create a data.frame for each table
for (i in seq(along=tables)) {
  if(tables[[i]] == 'NBASBHalfLines' | tables[[i]] == 'NBASBLines'){
  lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT away_team, home_team, game_date, line, spread, max(game_time) as
game_time from ", tables[[i]], " group by away_team, home_team, game_date;"))

  } else {
    lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT * FROM '", tables[[i]], "'", sep=""))
  }
  cat(tables[[i]], ":", i, "\n")
}

lines <- lDataFrames[[which(tables == "NBASBLines")]]
lookup <- lDataFrames[[which(tables == "NBASBTeamLookup")]]
halfbox <- lDataFrames[[which(tables == "NBAStats")]]
finalbox <- lDataFrames[[which(tables == "NBAfinalstats")]]
games <- lDataFrames[[which(tables == "NBAGames")]]

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
lines$spread <- as.numeric(gsub("\\+", "", lines$spread)) * -1

library(dplyr)

lines <- lines[c("game_id", "game_time", "away_espn", "home_espn", "line", "spread", "game_date")]
write.csv(lines, file="/home/ec2-user/sports2016/NBA/lines.csv", row.names=FALSE)

gs_auth(token = '/home/ec2-user/sports2016/NCF/ttt.rds')

ncf <- gs_key('17KG9hOzgGc9phDwbLrALWDdc1ugslD2vUYFMHS7qZtg', visibility = 'private')

gs_edit_cells(ncf, ws='pregame', input=colnames(lines), byrow=TRUE, anchor="A1")

lines <- lines[lines$game_date == format(Sys.Date(), "%m/%d/%Y"),]
gs_edit_cells(ncf, ws='pregame', input = lines, anchor="A2", col_names=FALSE, trim=TRUE)

#gs_upload("/home/ec2-user/sports2016/NBA/lines.csv", sheet_title = 'pregame')
#gs_add_row(ncf, ws='pregame', input = lines)
