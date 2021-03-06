library(randomForest)
library(data.table)
library(RSQLite)
#library(plyr)
library(dplyr)

#setwd("/home/ec2-user/sports2016/NCF/")

drv <- dbDriver("SQLite")
con <- dbConnect(drv, "/home/ec2-user/sports2016/NBA/sports.db")

tables <- dbListTables(con)

lDataFrames <- vector("list", length=length(tables))

## create a data.frame for each table
for (i in seq(along=tables)) {
  if(tables[[i]] == 'NBAHalflines' | tables[[i]] == 'NBAlines' | tables[[i]] == 'NBASBLines' | tables[[i]] == 'NBASBHalfLines'){
   lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste0("SELECT n.away_team, n.home_team, n.game_date, n.line, n.spread, n.game_time from '", tables[[i]], "' n inner join
  (select game_date, away_team,home_team, max(game_time) as mgt from '", tables[[i]], "' group by game_date, away_team, home_team) s2 on s2.game_date = n.game_date and
  s2.away_team = n.away_team and s2.home_team = n.home_team and n.game_time = s2.mgt;"))

  } else {
        lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT * FROM '", tables[[i]], "'", sep=""))
  }
  cat(tables[[i]], ":", i, "\n")
}

halflines <- lDataFrames[[which(tables == "NBASBHalfLines")]]
games <- lDataFrames[[which(tables == "NBAGames")]]
lines <- lDataFrames[[which(tables == "NBASBLines")]]
teamstats <- lDataFrames[[which(tables == "NBAseasonstats")]]
boxscores <- lDataFrames[[which(tables == "NBAStats")]]
lookup <- lDataFrames[[which(tables == "NBASBTeamLookup")]]
nbafinal <- lDataFrames[[which(tables == "NBAfinalstats")]]
seasontotals <- lDataFrames[[which(tables == "NBAseasontotals")]]

nbafinal <- nbafinal[order(as.Date(nbafinal$timestamp)),]
nbafinal$timestamp <- as.Date(nbafinal$timestamp)
d<-data.table(nbafinal, key="team")
d<-d[, tail(.SD, 7), by=team]
avg.points.last.7<-as.data.frame(d[, mean(pts, na.rm = TRUE),by = team])

b<-apply(boxscores[,3:5], 2, function(x) strsplit(x, "-"))
boxscores$fgm <- do.call("rbind",b$fgma)[,1]
boxscores$fga <- do.call("rbind",b$fgma)[,2]
boxscores$tpm <- do.call("rbind",b$tpma)[,1]
boxscores$tpa <- do.call("rbind",b$tpma)[,2]
boxscores$ftm <- do.call("rbind",b$ftma)[,1]
boxscores$fta <- do.call("rbind",b$ftma)[,2]
boxscores <- boxscores[,c(1,2,16:21,6:15)]

m1<-merge(boxscores, games, by="game_id")
m1$key <- paste(m1$team, m1$game_date)
teamstats$key <- paste(teamstats$team, teamstats$the_date)
m2<-merge(m1, teamstats, by="key")
lookup$away_team <- lookup$sb_team
lookup$home_team <- lookup$sb_team
#m2$home_team <- games[match(m2$game_id, games$game_id),]$team2

## Total Lines
lines$game_time<-as.POSIXlt(lines$game_time)
lines<-lines[order(lines$home_team, lines$game_time),]
lines$key <- paste(lines$away_team, lines$home_team, lines$game_date)

# grabs the first line value after 'OFF'
#res2 <- tapply(1:nrow(lines), INDEX=lines$key, FUN=function(idxs) idxs[lines[idxs,'line'] != 'OFF'][1])
#first<-lines[res2[which(!is.na(res2))],]
#lines <- first[,1:6]

## Merge line data with lookup table
la<-merge(lookup, lines, by="away_team")
lh<-merge(lookup, lines, by="home_team")
la$key <- paste(la$espn_abbr, la$game_date)
lh$key <- paste(lh$espn_abbr, lh$game_date)
m3a<-merge(m2, la, by="key")
m3h<-merge(m2, lh, by="key")
colnames(m3a)[49] <- "CoversTotalLineUpdateTime"
colnames(m3h)[49] <- "CoversTotalLineUpdateTime"

## Halftime Lines
halflines$game_time<-as.POSIXlt(halflines$game_time)
halflines<-halflines[order(halflines$home_team, halflines$game_time),]
halflines$key <- paste(halflines$away_team, halflines$home_team, halflines$game_date)

la2<-merge(lookup, halflines, by="away_team")
lh2<-merge(lookup, halflines, by="home_team")
la2$key <- paste(la2$espn_abbr, la2$game_date)
lh2$key <- paste(lh2$espn_abbr, lh2$game_date)
m3a2<-merge(m2, la2, by="key")
m3h2<-merge(m2, lh2, by="key")
colnames(m3a2)[49] <- "CoversHalfLineUpdateTime"
colnames(m3h2)[49] <- "CoversHalfLineUpdateTime"
l<-merge(m3a, m3a2, by=c("game_date.y", "away_team"))
#l<-l[match(m3a$key, l$key.y),]
m3a<-m3a[match(l$key.y, m3a$key),]
m3a<-cbind(m3a, l[,94:96])
l2<-merge(m3h, m3h2, by=c("game_date.y", "home_team"))
#l2<-l2[match(m3h$key, l2$key.y),]
m3h<-m3h[match(l2$key.y, m3h$key),]
m3h<-cbind(m3h, l2[,94:96])
colnames(m3h)[44:45] <- c("home_team.x", "home_team.y")
colnames(m3a)[40] <- "home_team"
#m2$newkey <- paste(m2$game_id, m2$team1, m2$team2)
if(dim(m3a)[1] > 0){
 m3a$hometeam <- FALSE
 m3h$hometeam <- TRUE
 m3h <- m3h[,1:53]
}

m3a <- unique(m3a)
m3h <- unique(m3h)

halftime_stats<-rbind(m3a,m3h)
if(length(which(halftime_stats$game_id %in% names(which(table(halftime_stats$game_id) != 2))) > 0)){
halftime_stats<-halftime_stats[-which(halftime_stats$game_id %in% names(which(table(halftime_stats$game_id) != 2)) ),]
}
#halftime_stats <- subset(halftime_stats, line.y != 'OFF')
halftime_stats<-halftime_stats[which(!is.na(halftime_stats$line.y)),]
halftime_stats<-halftime_stats[order(halftime_stats$game_id),]
halftime_stats$CoversTotalLineUpdateTime <- as.character(halftime_stats$CoversTotalLineUpdateTime)
halftime_stats$CoversHalfLineUpdateTime<-as.character(halftime_stats$CoversHalfLineUpdateTime)

#diffs<-ddply(halftime_stats, .(game_id), transform, diff=pts.x[1] - pts.x[2])
if(dim(halftime_stats)[1] > 0 ){
halftime_stats$half_diff <-  rep(aggregate(pts ~ game_id, data=halftime_stats, FUN=diff)[,2] * -1, each=2)
halftime_stats$line.y<-as.numeric(halftime_stats$line.y)
halftime_stats$line <- as.numeric(halftime_stats$line)
halftime_stats$mwt<-rep(aggregate(pts ~ game_id, data=halftime_stats, sum)[,2], each=2) + halftime_stats$line.y - halftime_stats$line
half_stats <- halftime_stats[seq(from=2, to=dim(halftime_stats)[1], by=2),]
} else {
  return(data.frame(results="No Results"))
}

all <- rbind(m3a, m3h)
all <- all[,-1]
all$key <- paste(all$game_id, all$team.y)
all<-all[match(unique(all$key), all$key),]

colnames(all) <- c("GAME_ID","TEAM","HALF_FGM", "HALF_FGA", "HALF_3PM","HALF_3PA", "HALF_FTM","HALF_FTA","HALF_OREB", "HALF_DREB", "HALF_REB",
"HALF_AST", "HALF_STL", "HALF_BLK", "HALF_TO", "HALF_PF", "HALF_PTS", "HALF_TIMESTAMP", "TEAM1", "TEAM2", "GAME_DATE","GAME_TIME",
"REMOVE2","REMOVE3","SEASON_FGM","SEASON_FGA", "SEASON_FGP","SEASON_3PM", "SEASON_3PA", "SEASON_3PP", "SEASON_FTM","SEASON_FTA","SEASON_FTP",
"SEASON_2PM", "SEASON_2PA", "SEASON_2PP","SEASON_PPS", "SEASON_AFG","REMOVE4", "REMOVE5", "REMOVE6", "REMOVE7","REMOVE8", "REMOVE9", "REMOVE10",
"LINE", "SPREAD", "COVERS_UPDATE","LINE_HALF", "SPREAD_HALF", "COVERS_HALF_UPDATE", "HOME_TEAM", "REMOVE11")
all <- all[,-grep("REMOVE", colnames(all))]

## Add the season total stats
colnames(seasontotals)[1] <- "TEAM"
colnames(seasontotals)[2] <- "GAME_DATE"
all$key <- paste(all$GAME_DATE, all$TEAM)
seasontotals$key <- paste(seasontotals$GAME_DATE, seasontotals$TEAM)

x <- cbind(all, seasontotals[match(all$key, seasontotals$key),])
final<-x[,c(1:57)]
colnames(final)[47:57] <- c("SEASON_GP", "SEASON_PPG", "SEASON_ORPG", "SEASON_DEFRPG", "SEASON_RPG", "SEASON_APG", "SEASON_SPG", "SEASON_BGP",
"SEASON_TPG", "SEASON_FPG", "SEASON_ATO")
final<-final[order(final$GAME_DATE, decreasing=TRUE),]

## match half stats that have 2nd half lines with final set
f<-final[which(final$GAME_ID %in% half_stats$game_id),]

nbafinal$key <- paste(nbafinal$game_id, nbafinal$team)
n<-apply(nbafinal[,3:5], 2, function(x) strsplit(x, "-"))
nbafinal$fgm <- do.call("rbind",n$fgma)[,1]
nbafinal$fga <- do.call("rbind",n$fgma)[,2]
nbafinal$tpm <- do.call("rbind",n$tpma)[,1]
nbafinal$tpa <- do.call("rbind",n$tpma)[,2]
nbafinal$ftm <- do.call("rbind",n$ftma)[,1]
nbafinal$fta <- do.call("rbind",n$ftma)[,2]
nbafinal <- nbafinal[,c(1,2,17:22,6:16)]

f$key <- paste(f$GAME_ID, f$TEAM)
f<-cbind(f, nbafinal[match(f$key, nbafinal$key),])
all <- f

#all <- all[-match(names(which(table(all$GAME_ID) != 2)), all$GAME_ID),]
all <- all[order(all$GAME_ID),]
all$home_team <- games[match(all$game_id, games$game_id),]$team2
all$the_team <- ""
all[seq(from=1, to=dim(all)[1], by=2),]$the_team <- "TEAM1"
all[seq(from=2, to=dim(all)[1], by=2),]$the_team <- "TEAM2"

wide <- reshape(all, direction = "wide", idvar="GAME_ID", timevar="the_team")

result <- wide
result$GAME_DATE <- strptime(substr(paste(result$GAME_DATE.TEAM1, result$GAME_TIME.TEAM1), 0, 19), format='%m/%d/%Y %H:%M %p')
result$GAME_DATE <- strftime(result$GAME_DATE, format='%m/%d/%Y %H:%M %p')

result$HALF_FGA.TEAM1 <- as.numeric(result$HALF_FGA.TEAM1)
result$HALF_FTA.TEAM1 <- as.numeric(result$HALF_FTA.TEAM1)
result$HALF_TO.TEAM1 <- as.numeric(result$HALF_TO.TEAM1)
result$HALF_OREB.TEAM1 <- as.numeric(result$HALF_OREB.TEAM1)
result$HALF_FGA.TEAM2 <- as.numeric(result$HALF_FGA.TEAM2)
result$HALF_FTA.TEAM2 <- as.numeric(result$HALF_FTA.TEAM2)
result$HALF_TO.TEAM2 <- as.numeric(result$HALF_TO.TEAM2)
result$HALF_OREB.TEAM2 <- as.numeric(result$HALF_OREB.TEAM2)

result$possessions.TEAM1 <- result$HALF_FGA.TEAM1 + (result$HALF_FTA.TEAM1 / 2) + result$HALF_TO.TEAM1 - result$HALF_OREB.TEAM1
result$possessions.TEAM2 <- result$HALF_FGA.TEAM2 + (result$HALF_FTA.TEAM2 / 2) + result$HALF_TO.TEAM2 - result$HALF_OREB.TEAM2

result$fga.TEAM1 <- as.numeric(result$fga.TEAM1)
result$fga.TEAM2 <- as.numeric(result$fga.TEAM2)
result$fta.TEAM1 <- as.numeric(result$fta.TEAM1)
result$fta.TEAM2 <- as.numeric(result$fta.TEAM2)
result$turnovers.TEAM1 <- as.numeric(result$turnovers.TEAM1)
result$turnovers.TEAM2 <- as.numeric(result$turnovers.TEAM2)
result$oreb.TEAM1 <- as.numeric(result$oreb.TEAM1)
result$oreb.TEAM2 <- as.numeric(result$oreb.TEAM2)

result$final.possessions.TEAM1 <- result$fga.TEAM1 + (result$fta.TEAM1 / 2) + result$turnovers.TEAM1 - result$oreb.TEAM1
result$final.possessions.TEAM2 <- result$fga.TEAM2 + (result$fta.TEAM2 / 2) + result$turnovers.TEAM2 - result$oreb.TEAM2

result <- subset(result, select=c(GAME_ID, GAME_DATE, TEAM.TEAM1, TEAM.TEAM2, HALF_PTS.TEAM1, HALF_PTS.TEAM2, LINE_HALF.TEAM1, SPREAD_HALF.TEAM1, LINE.TEAM1, SPREAD.TEAM1, possessions.TEAM1, 
					possessions.TEAM2, final.possessions.TEAM1,final.possessions.TEAM2, pts.TEAM1, pts.TEAM2))
result$GAME_DATE <- strptime(result$GAME_DATE, format="%m/%d/%Y %H:%M")
result <- result[order(result$GAME_DATE, decreasing=TRUE),]
result$GAME_DATE <- as.character(result$GAME_DATE)

library(googlesheets)
library(dplyr)

gs_auth(token = '/home/ec2-user/sports2016/NCF/ttt.rds')

ncf <- gs_key('17KG9hOzgGc9phDwbLrALWDdc1ugslD2vUYFMHS7qZtg', visibility = 'private')

gs_edit_cells(ncf, ws='postgame', input=colnames(result), byrow=TRUE, anchor="A1")

#result <- result[result$GAME_DATE == format(Sys.Date()-1, "%m/%d/%Y"),]

result <- result[as.Date(result$GAME_DATE) == format(Sys.Date()-1, "%Y-%m-%d"),]

gs_edit_cells(ncf, ws='postgame', input = result, anchor="A2", col_names=FALSE, trim=TRUE)

