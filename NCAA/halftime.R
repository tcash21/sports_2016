library(randomForest)
#library(data.table)
library(RSQLite)
library(dplyr)
library(data.table)

#setwd("/home/ec2-user/sports2016/NCF/")

drv <- dbDriver("SQLite")
con <- dbConnect(drv, "/home/ec2-user/sports2016/NCAA/sports.db")

tables <- dbListTables(con)

lDataFrames <- vector("list", length=length(tables))


## create a data.frame for each table
for (i in seq(along=tables)) {
  if(tables[[i]] == 'NCAASBLines' | tables[[i]] == 'NCAASBHalfLines'){
  lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT away_team, home_team, game_date, line, spread, max(game_time) as
game_time from ", tables[[i]], " group by away_team, home_team, game_date;"))
  } else {
	lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT * FROM '", tables[[i]], "'", sep=""))
  }
  cat(tables[[i]], ":", i, "\n")
}

halflines <- lDataFrames[[which(tables == "NCAASBHalfLines")]]
games <- lDataFrames[[which(tables == "NCAAgames")]]
lines <- lDataFrames[[which(tables == "NCAASBLines")]]
teamstats <- lDataFrames[[which(tables == "NCAAseasonstats")]]
boxscores <- lDataFrames[[which(tables == "NCAAstats")]]
lookup <- lDataFrames[[which(tables == "NCAASBTeamLookup")]]
ncaafinal <- lDataFrames[[which(tables == "NCAAfinalstats")]]
seasontotals <- lDataFrames[[which(tables == "NCAAseasontotals")]]

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


## Total Lines
lines$game_time<-as.POSIXlt(lines$game_time)
lines<-lines[order(lines$home_team, lines$game_time),]
lines$key <- paste(lines$away_team, lines$home_team, lines$game_date)

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
colnames(m3a)[41] <- "home_team"
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
halftime_stats$half_diff <-  rep(aggregate(pts.x ~ game_id, data=halftime_stats, FUN=diff)[,2] * -1, each=2)
halftime_stats$line.y<-as.numeric(halftime_stats$line.y)
halftime_stats$line <- as.numeric(halftime_stats$line)
halftime_stats$mwt<-rep(aggregate(pts.x ~ game_id, data=halftime_stats, sum)[,2], each=2) + halftime_stats$line.y - halftime_stats$line
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

all <- all[-match(names(which(table(all$GAME_ID) != 2)), all$GAME_ID),]
all <- all[order(all$GAME_ID),]
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

result <- subset(result, select=c(GAME_ID, GAME_DATE, TEAM.TEAM1, TEAM.TEAM2, HALF_PTS.TEAM1, HALF_PTS.TEAM2, LINE_HALF.TEAM1, SPREAD_HALF.TEAM1, LINE.TEAM1, SPREAD.TEAM1, possessions.TEAM1, 
                possessions.TEAM2))


colnames(result) <- c("GAME_ID", "GAME_DATE", "TEAM1", "TEAM2", "HALF_PTS.TEAM1", "HALF_PTS.TEAM2", "LINE_HALF", "SPREAD_HALF", "LINE", "SPREAD", "possessions.TEAM1", "possessions.TEAM2")
#all <- subset(all, select=c(game_id, game_date.x, game_time.x, team1, team2, line, spread, pts.x, pts.y))

library(googlesheets)
library(dplyr)

gs_auth(token = '/home/ec2-user/sports2016/NCF/ttt.rds')

ncf <- gs_key('1D_uKs4UuAkM4rijIphX-0x2gdqT4wgsXCdC1nTLnMMA', visibility = 'private')

today <- format(Sys.Date(), "%m/%d/%Y")
tomorrow <- format(Sys.Date() + 1, "%m/%d/%Y")
yesterday <- format(Sys.Date() - 1, "%m/%d/%Y")
the_data1 <- result[grep(today, result$GAME_DATE),]
the_data2 <- result[grep(tomorrow, result$GAME_DATE),]
the_data3 <- result[grep(yesterday, result$GAME_DATE),]
result <- rbind(the_data1, the_data2, the_data3)
result$temp<-strptime(result$GAME_DATE, format='%m/%d/%Y %H:%M')
result <- result[order(result$temp, decreasing=TRUE),]
result <- subset(result, select=c(-temp))

gs_edit_cells(ncf, ws='halftime', input=colnames(result), byrow=TRUE, anchor="A1")

gs_edit_cells(ncf, ws='halftime', input = result, anchor="A2", col_names=FALSE, trim=TRUE)

