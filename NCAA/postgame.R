library(randomForest)
library(data.table)
library(RSQLite)
library(plyr)

#setwd("/home/ec2-user/sports2016/NCF/")

drv <- dbDriver("SQLite")
con <- dbConnect(drv, "/home/ec2-user/sports2016/NCAA/sports.db")

tables <- dbListTables(con)

lDataFrames <- vector("list", length=length(tables))

## create a data.frame for each table
for (i in seq(along=tables)) {
  if(tables[[i]] == 'NCAAHalflines' | tables[[i]] == 'NCAAlines' | tables[[i]] == 'NCAASBLines' | tables[[i]] == 'NCAASBHalfLines'){
   lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste0("SELECT n.away_team, n.home_team, n.game_date, n.line, n.spread, n.game_time from '", tables[[i]], "' n inner join
  (select game_date, away_team,home_team, max(game_time) as mgt from '", tables[[i]], "' group by game_date, away_team, home_team) s2 on s2.game_date = n.game_date and
  s2.away_team = n.away_team and s2.home_team = n.home_team and n.game_time = s2.mgt;"))

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


ncaafinal$key <- paste(ncaafinal$game_id, ncaafinal$team)
n<-apply(ncaafinal[,3:5], 2, function(x) strsplit(x, "-"))
ncaafinal$fgm <- do.call("rbind",n$fgma)[,1]
ncaafinal$fga <- do.call("rbind",n$fgma)[,2]
ncaafinal$tpm <- do.call("rbind",n$tpma)[,1]
ncaafinal$tpa <- do.call("rbind",n$tpma)[,2]
ncaafinal$ftm <- do.call("rbind",n$ftma)[,1]
ncaafinal$fta <- do.call("rbind",n$ftma)[,2]
ncaafinal <- ncaafinal[,c(1,2,17:22,6:16)]

final<-merge(ncaafinal, all, by="key")
final <- final[,-1]

colnames(final) <- c("GAME_ID","TEAM","FINAL_FGM","FINAL_FGA", "FINAL_3PM","FINAL_3PA","FINAL_FTM","FINAL_FTA","FINAL_OREB","FINAL_DREB","FINAL_REB",
"FINAL_AST","FINAL_STL","FINAL_BLK","FINAL_TO","FINAL_PF","FINAL_PTS","FINAL_BOXSCORE_TIMESTAMP", "REMOVE0","REMOVE1","HALF_FGM", "HALF_FGA", "HALF_3PM",
"HALF_3PA", "HALF_FTM","HALF_FTA","HALF_OREB", "HALF_DREB", "HALF_REB", "HALF_AST", "HALF_STL", "HALF_BLK", "HALF_TO", "HALF_PF", "HALF_PTS",
"HALF_TIMESTAMP", "TEAM1", "TEAM2", "GAME_DATE","GAME_TIME","REMOVE2","REMOVE3","MIN", "SEASON_FGM","SEASON_FGA","SEASON_FTM","SEASON_FTA","SEASON_3PM",
"SEASON_3PA","SEASON_PTS","SEASON_OFFR","SEASON_DEFR","SEASON_REB","SEASON_AST","SEASON_TO","SEASON_STL", "SEASON_BLK","REMOVE4","REMOVE5","REMOVE6",
"REMOVE7","REMOVE8","REMOVE9","LINE", "SPREAD", "COVERS_UPDATE","LINE_HALF", "SPREAD_HALF", "COVERS_HALF_UPDATE")
final <- final[,-grep("REMOVE", colnames(final))]

## Add the season total stats
colnames(seasontotals)[1] <- "TEAM"
colnames(seasontotals)[2] <- "GAME_DATE"
#today <- format(Sys.Date(), "%m/%d/%Y")
#seasontotals <- subset(seasontotals, GAME_DATE == today)
final$key <- paste(final$GAME_DATE, final$TEAM)
seasontotals$key <- paste(seasontotals$GAME_DATE, seasontotals$TEAM)

x<-merge(seasontotals, final, by=c("key"))
x<- x[,c(-1, -16, -51)]
final<-x[,c(14:51, 1:3,5:13, 52:71)]
colnames(final)[c(41:50,70)] <- c("SEASON_GP", "SEASON_PPG", "SEASON_RPG", "SEASON_APG", "SEASON_SPG", "SEASON_BPG", "SEASON_TPG", "SEASON_FGP",
"SEASON_FTP", "SEASON_3PP", "HOME_TEAM")
#final$GAME_DATE <- seasontotals$GAME_DATE[1]
#final$GAME_DATE<-games[match(final$GAME_ID, games$game_id),]$game_date
final<-final[order(final$GAME_DATE, decreasing=TRUE),]
final$LINE_HALF <- as.numeric(final$LINE_HALF)
final<-final[-which(is.na(final$LINE_HALF)),]

final$LINE <- as.numeric(final$LINE)
final$COVERS_UPDATE<-as.character(final$COVERS_UPDATE)
num_games<-data.frame(table(final$GAME_ID))
game_ids <- num_games[-which(num_games$Freq != 2),]$Var1
final <- final[which(final$GAME_ID %in% game_ids),]
final<-final[order(final$GAME_ID),]

final$COVERS_HALF_UPDATE<-as.POSIXct(final$COVERS_HALF_UPDATE)

final<-ddply(final, .(GAME_ID), transform, mwt=HALF_PTS[1] + HALF_PTS[2] + LINE_HALF - LINE)
final <- ddply(final, .(GAME_ID), transform, half_diff=HALF_PTS[1] - HALF_PTS[2])

## transform to numerics
final[,2:16]<-apply(final[,2:16], 2, as.numeric)
final[,18:32]<-apply(final[,18:32], 2, as.numeric)
final[,c(38,41:63)]<-apply(final[,c(38,41:63)], 2, as.numeric)

result <- final
result$team <- ""
result[seq(from=1, to=dim(result)[1], by=2),]$team <- "TEAM1"
result[seq(from=2, to=dim(result)[1], by=2),]$team <- "TEAM2"
wide<-reshape(result, direction = "wide", idvar="GAME_ID", timevar="team")
wide$secondHalfPts.TEAM1 <- wide$FINAL_PTS.TEAM1 - wide$HALF_PTS.TEAM1
wide$secondHalfPts.TEAM2 <- wide$FINAL_PTS.TEAM2 - wide$HALF_PTS.TEAM2
wide$secondHalfPtsTotal <- wide$secondHalfPts.TEAM1 + wide$secondHalfPts.TEAM2
wide$Over<-wide$secondHalfPtsTotal > wide$LINE_HALF.TEAM1

wide$SPREAD_HALF.TEAM1<-as.numeric(wide$SPREAD_HALF.TEAM1)
wide$SPREAD.TEAM1 <- as.numeric(wide$SPREAD.TEAM1)
wide$FGS_GROUP <- NA
if(length(which(abs(wide$SPREAD.TEAM1) < 2.1)) > 0){
wide[which(abs(wide$SPREAD.TEAM1) < 2.1),]$FGS_GROUP <- '1'
}
if(length(which(abs(wide$SPREAD.TEAM1) >= 2.1 & abs(wide$SPREAD.TEAM1) < 5.1)) > 0){
wide[which(abs(wide$SPREAD.TEAM1) >= 2.1 & abs(wide$SPREAD.TEAM1) < 5.1),]$FGS_GROUP <- '2'
}
if(length(which(abs(wide$SPREAD.TEAM1) >= 5.1 & abs(wide$SPREAD.TEAM1) < 9.1)) > 0){
wide[which(abs(wide$SPREAD.TEAM1) >= 5.1 & abs(wide$SPREAD.TEAM1) < 9.1),]$FGS_GROUP <- '3'
}
if(length(which(abs(wide$SPREAD.TEAM1) >= 9.1 & abs(wide$SPREAD.TEAM1) < 14.1)) > 0){
wide[which(abs(wide$SPREAD.TEAM1) >= 9.1 & abs(wide$SPREAD.TEAM1) < 14.1),]$FGS_GROUP <- '4'
}
if(length(which(abs(wide$SPREAD.TEAM1) > 14.1)) > 0){
  wide[which(abs(wide$SPREAD.TEAM1) > 14.1),]$FGS_GROUP <- '5'
}

wide$LINE_HALF.TEAM1<-as.numeric(wide$LINE_HALF.TEAM1)
wide$HALF_DIFF <- NA
wide$underDog.TEAM1 <- (wide$HOME_TEAM.TEAM1 == FALSE & wide$SPREAD.TEAM1 > 0) | (wide$HOME_TEAM.TEAM1 == TRUE & wide$SPREAD.TEAM1 < 0)
under.teams <- which(wide$underDog.TEAM1)
favorite.teams <- which(!wide$underDog.TEAM1)
wide[under.teams,]$HALF_DIFF <- wide[under.teams,]$HALF_PTS.TEAM2 - wide[under.teams,]$HALF_PTS.TEAM1
wide[favorite.teams,]$HALF_DIFF <- wide[favorite.teams,]$HALF_PTS.TEAM1 - wide[favorite.teams,]$HALF_PTS.TEAM2
wide$MWTv2 <- wide$LINE_HALF.TEAM1 - (wide$LINE.TEAM1 /2)
wide$poss.TEAM1 <- wide$HALF_FGA.TEAM1 + (wide$HALF_FTA.TEAM1 / 2) + wide$HALF_TO.TEAM1 - wide$HALF_OREB.TEAM1
wide$poss.TEAM2 <- wide$HALF_FGA.TEAM2 + (wide$HALF_FTA.TEAM2 / 2) + wide$HALF_TO.TEAM2 - wide$HALF_OREB.TEAM2

#wide$SEASON_ORPG.TEAM1<-wide$SEASON_OFFR.TEAM1 / wide$SEASON_GP.TEAM1
#wide$SEASON_ORPG.TEAM2<-wide$SEASON_OFFR.TEAM2 / wide$SEASON_GP.TEAM2
#wide$possessions.TEAM1.SEASON <- (wide$SEASON_FGA.TEAM1 / wide$SEASON_GP.TEAM1) + ((wide$SEASON_FTA.TEAM1 / wide$SEASON_GP.TEAM1) / 2) + wide$SEASON_TPG.TEAM1 - wide$SEASON_ORPG.TEAM1
#wide$possessions.TEAM2.SEASON <- (wide$SEASON_FGA.TEAM2 / wide$SEASON_GP.TEAM2) + ((wide$SEASON_FTA.TEAM2 / wide$SEASON_GP.TEAM2) / 2) + wide$SEASON_TPG.TEAM2 - wide$SEASON_ORPG.TEAM2
#wide$POSSvE <- NA

## Adjust this for Fav and Dog
#possvEau <- ((wide[under.teams,]$possessions.TEAM2 + wide[under.teams,]$poss.TEAM1) / 2)
#possvEbu <- ((wide[under.teams,]$possessions.TEAM2.SEASON / 2 - 1 + wide[under.teams,]$possessions.TEAM1.SEASON / 2 - 1) / 2)
#wide[under.teams,]$POSSvE <- possvEau - possvEbu
#possvEfau <- ((wide[favorite.teams,]$possessions.TEAM1 + wide[favorite.teams,]$poss.TEAM2) / 2)
#possvEfbu <- ((wide[favorite.teams,]$possessions.TEAM1.SEASON / 2 - 1 + wide[favorite.teams,]$possessions.TEAM2.SEASON / 2 - 1) / 2)
#wide[favorite.teams,]$POSSvE <- possvEfau - possvEfbu
#wide$P100vE <- NA
#wide$P100.TEAM1 <- wide$HALF_PTS.TEAM1 / wide$possessions.TEAM1 * 100
#wide$P100.TEAM1.SEASON <- wide$SEASON_PPG.TEAM1 / wide$possessions.TEAM1.SEASON * 100
#wide$P100.TEAM2 <- wide$HALF_PTS.TEAM2 / wide$possessions.TEAM2 * 100
#wide$P100.TEAM2.SEASON <- wide$SEASON_PPG.TEAM2 / wide$possessions.TEAM2.SEASON * 100

#wide$P100_DIFF <- NA
#wide[under.teams,]$P100_DIFF <- (wide[under.teams,]$P100.TEAM2 - wide[under.teams,]$P100.TEAM2.SEASON - 8) - (wide[under.teams,]$P100.TEAM1 - wide[under.teams,]$P100.TEAM1.SEASON - 9)
#wide[favorite.teams,]$P100_DIFF <- (wide[favorite.teams,]$P100.TEAM1 - wide[favorite.teams,]$P100.TEAM1.SEASON - 8) - (wide[favorite.teams,]$P100.TEAM2 - wide[favorite.teams,]$P100.TEAM2.SEASON - 9)

#wide[favorite.teams,]$P100vE <- (wide[favorite.teams,]$P100.TEAM1 - wide[favorite.teams,]$P100.TEAM1.SEASON - 8) + (wide[favorite.teams,]$P100.TEAM2 - wide[favorite.teams,]$P100.TEAM2.SEASON - 9)
#wide[under.teams,]$P100vE <- (wide[under.teams,]$P100.TEAM2 - wide[under.teams,]$P100.TEAM2.SEASON - 8) + (wide[under.teams,]$P100.TEAM1 - wide[under.teams,]$P100.TEAM1.SEASON - 9)

#wide$prediction<-predict(rpart.model,newdata=wide, type="class")
#wide$FAV <- ""
#wide[which(wide$underDog.TEAM1),]$FAV <- wide[which(wide$underDog.TEAM1),]$TEAM.x.TEAM2
#wide[which(!wide$underDog.TEAM1),]$FAV <- wide[which(!wide$underDog.TEAM1),]$TEAM.x.TEAM1
#wide$MWTv3 <- 0

#i <- which(wide$SPREAD.TEAM1 > 0)
#wide$MWTv3[i] <- wide[i,]$SPREAD_HALF.TEAM1 - (wide[i,]$SPREAD.TEAM1 / 2)

#i <- which(wide$SPREAD.TEAM1 <= 0)
#wide$MWTv3[i] <- -wide[i,]$SPREAD_HALF.TEAM1 + (wide[i,]$SPREAD.TEAM1 / 2)
#wide$MWT <- wide$HALF_PTS.TEAM1 + wide$HALF_PTS.TEAM2 + wide$LINE_HALF.TEAM1 - wide$LINE.TEAM1

#colnames(wide)[grep("MWTv2", colnames(wide))] <- '2H_LD'
#colnames(wide)[grep("MWTv3", colnames(wide))] <- '2H_SD'

wide$final.poss.TEAM1 <- wide$FINAL_FGA.TEAM1 + (wide$FINAL_FTA.TEAM1 / 2) + wide$FINAL_TO.TEAM1 - wide$FINAL_OREB.TEAM1
wide$final.poss.TEAM2 <- wide$FINAL_FGA.TEAM2 + (wide$FINAL_FTA.TEAM2 / 2) + wide$FINAL_TO.TEAM2 - wide$FINAL_OREB.TEAM2

wide <- subset(wide, select=c(GAME_ID, GAME_DATE.x.TEAM2, TEAM.x.TEAM1, TEAM.x.TEAM2, poss.TEAM1, poss.TEAM2, final.poss.TEAM1, final.poss.TEAM2, HALF_PTS.TEAM1, HALF_PTS.TEAM2, 
				FINAL_PTS.TEAM1, FINAL_PTS.TEAM2))
colnames(wide) <- c("GAME_ID", "GAME_DATE", "TEAM1", "TEAM2", "HALF_POSS.TEAM1", "HALF_POSS.TEAM2", "FINAL_POSS.TEAM1", "FINAL_POSS.TEAM2", "HALF_PTS.TEAM1", "HALF_PTS.TEAM2", 
		"FINAL_PTS.TEAM1", "FINAL_PTS.TEAM2")

wide$temp<-strptime(wide$GAME_DATE, format='%m/%d/%Y')
wide <- wide[order(wide$temp, decreasing=TRUE),]
wide <- subset(wide, select=c(-temp))

library(googlesheets)
library(dplyr)

gs_auth(token = '/home/ec2-user/sports2016/NCF/ttt.rds')

ncf <- gs_key('1D_uKs4UuAkM4rijIphX-0x2gdqT4wgsXCdC1nTLnMMA', visibility = 'private')

gs_edit_cells(ncf, ws='postgame', input=colnames(wide), byrow=TRUE, anchor="A1")

gs_edit_cells(ncf, ws='postgame', input = wide, anchor="A2", col_names=FALSE, trim=TRUE)

