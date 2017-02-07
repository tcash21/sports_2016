library(data.table)
library(randomForest)
library(plyr)
library(RSQLite)
library(sendmailR)

drv <- dbDriver("SQLite")
con <- dbConnect(drv, "/home/ec2-user/sports2015/NBA/sports.db")

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
boxscores <- lDataFrames[[which(tables == "NBAstats")]]
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

# grabs first line value after 'OFF'
#res2 <- tapply(1:nrow(halflines), INDEX=halflines$key, FUN=function(idxs) idxs[halflines[idxs,'line'] != 'OFF'][1])
#first<-halflines[res2[which(!is.na(res2))],]
#halflines <- first[,1:6]

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

## HOME/AWAY gets screwed up in this merge
#x<-merge(seasontotals, all, by=c("key"))
x <- cbind(all, seasontotals[match(all$key, seasontotals$key),])
#x<- x[,c(-1,-2, -16, -35)]
final<-x[,c(1:57)]
colnames(final)[47:57] <- c("SEASON_GP", "SEASON_PPG", "SEASON_ORPG", "SEASON_DEFRPG", "SEASON_RPG", "SEASON_APG", "SEASON_SPG", "SEASON_BGP",
"SEASON_TPG", "SEASON_FPG", "SEASON_ATO")
final<-final[order(final$GAME_DATE, decreasing=TRUE),]

## match half stats that have 2nd half lines with final set
f<-final[which(final$GAME_ID %in% half_stats$game_id),]
f$mwt <- half_stats[match(f$GAME_ID, half_stats$game_id),]$mwt
f$half_diff <- half_stats[match(f$GAME_ID, half_stats$game_id),]$half_diff
f[,3:17] <- apply(f[,3:17], 2, function(x) as.numeric(as.character(x)))
f[,23:37] <- apply(f[,23:37], 2, function(x) as.numeric(as.character(x)))
f[,47:57] <- apply(f[,47:57], 2, function(x) as.numeric(as.character(x)))
f[,58:59] <- apply(f[,58:59], 2, function(x) as.numeric(as.character(x)))

## Team1 and Team2 Halftime Differentials
f <- f[order(f$GAME_ID),]
f$fg_percent <- ((f$HALF_FGM / f$HALF_FGA) - (f$SEASON_FGM / f$SEASON_FGA))
f$FGM <- (f$HALF_FGM - (f$SEASON_FGM / f$SEASON_GP / 2))
f$TPM <- (f$HALF_3PM - (f$SEASON_3PM / f$SEASON_GP / 2))
f$FTM <- (f$HALF_FTM - (f$SEASON_FTM / f$SEASON_GP / 2 - 1))
f$TO <- (f$HALF_TO - (f$SEASON_ATO / 2))
f$OREB <- (f$HALF_OREB - (f$SEASON_ORPG / 2))

## Cumulative Halftime Differentials
f$COVERS_UPDATE<-as.character(f$COVERS_UPDATE)
f$COVERS_HALF_UPDATE <- as.character(f$COVERS_HALF_UPDATE)

f$chd_fg<-rep(aggregate(fg_percent ~ GAME_ID, data=f, function(x) sum(x) / 2)[,2], each=2)
f$chd_fgm <- rep(aggregate(FGM ~ GAME_ID, data=f, function(x) sum(x) / 2)[,2], each=2)
f$chd_tpm <- rep(aggregate(TPM ~ GAME_ID, data=f, function(x) sum(x) / 2)[,2], each=2)
f$chd_ftm <- rep(aggregate(FTM ~ GAME_ID, data=f, function(x) sum(x) / 2)[,2], each=2)
f$chd_to <- rep(aggregate(TO ~ GAME_ID, data=f, function(x) sum(x) / 2)[,2], each=2)
f$chd_oreb <- rep(aggregate(OREB ~ GAME_ID, data=f, function(x) sum(x) / 2)[,2], each=2)

## load nightly model trained on all previous data
## load("~/sports/models/NBAhalftimeOversModel.Rdat")


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
f <- f[,-72:-73]
colnames(f)[72:86]<-paste0("FINAL_", colnames(f)[72:86])

f<-f[order(f$GAME_ID),]

f$team <- ""
f[seq(from=1, to=dim(f)[1], by=2),]$team <- "TEAM1"
f[seq(from=2, to=dim(f)[1], by=2),]$team <- "TEAM2"


wide <- reshape(f, direction = "wide", idvar="GAME_ID", timevar="team")

result <- wide
result$GAME_DATE<- strptime(paste(result$GAME_DATE.1.TEAM1, result$GAME_TIME.TEAM1), format="%m/%d/%Y %I:%M %p")

colnames(result)[58] <- "MWT"
colnames(result)[38] <- "SPREAD"
colnames(result)[66:71] <- c("chd_fg", "chd_fgm", "chd_tpm", "chd_ftm", "chd_to", "chd_oreb")
result$SPREAD <- as.numeric(result$SPREAD)

result$mwtO <- as.numeric(result$MWT < 7.1 & result$MWT > -3.9)
result$chd_fgO <- as.numeric(result$chd_fg < .15 & result$chd_fg > -.07)
result$chd_fgmO <- as.numeric(result$chd_fgm < -3.9)
result$chd_tpmO <- as.numeric(result$chd_tpm < -1.9)
result$chd_ftmO <- as.numeric(result$chd_ftm < -.9)
result$chd_toO <- as.numeric(result$chd_to < -1.9)

result$mwtO[is.na(result$mwtO)] <- 0
result$chd_fgO[is.na(result$chd_fgO)] <- 0
result$chd_fgmO[is.na(result$chd_fgmO)] <- 0
result$chd_tpmO[is.na(result$chd_tpmO)] <- 0
result$chd_ftmO[is.na(result$chd_ftmO)] <- 0
result$chd_toO[is.na(result$chd_toO)] <- 0
result$overSum <- result$mwtO + result$chd_fgO + result$chd_fgmO + result$chd_tpmO + result$chd_ftmO + result$chd_toO

result$fullSpreadU <- as.numeric(abs(result$SPREAD) > 10.9)
result$mwtU <- as.numeric(result$MWT > 7.1)
result$chd_fgU <- as.numeric(result$chd_fg > .15 | result$chd_fg < -.07)
result$chd_fgmU <- 0
result$chd_tpmU <- 0
result$chd_ftmU <- as.numeric(result$chd_ftm > -0.9)
result$chd_toU <- as.numeric(result$chd_to > -1.9)

result$mwtU[is.na(result$mwtU)] <- 0
result$chd_fgO[is.na(result$chd_fgU)] <- 0
result$chd_fgmU[is.na(result$chd_fgmU)] <- 0
result$chd_tpmU[is.na(result$chd_tpmU)] <- 0
result$chd_ftmU[is.na(result$chd_ftmU)] <- 0
result$chd_toU[is.na(result$chd_toU)] <- 0
result$underSum <- result$fullSpreadU + result$mwtU + result$chd_fgU + result$chd_fgmU + result$chd_tpmU + result$chd_ftmU + result$chd_toU

result <- result[order(result$GAME_DATE),]
result$GAME_DATE <- as.character(result$GAME_DATE)
colnames(result)[67] <- 'chd_fg.TEAM1'
load("~/sports2015/NBA/randomForestModel.Rdat")

#colnames(result)[120] <- "mwt.TEAM1"
colnames(result)[which(colnames(result) == 'chd_fgm')] <- 'chd_fgm.TEAM1'
colnames(result)[which(colnames(result) == 'chd_fg')] <- 'chd_fg.TEAM1'
colnames(result)[which(colnames(result) == 'chd_ftm')] <- 'chd_ftm.TEAM1'
colnames(result)[which(colnames(result) == 'chd_to')] <- 'chd_to.TEAM1'
colnames(result)[which(colnames(result) == 'chd_oreb')] <- 'chd_oreb.TEAM1'
colnames(result)[which(colnames(result) == 'chd_tpm')] <- 'chd_tpm.TEAM1'


result$SPREAD_HALF.TEAM1<-as.numeric(result$SPREAD_HALF.TEAM1)

result$FGS_GROUP <- NA
if(length(which(abs(result$SPREAD) < 3.1)) > 0){
result[which(abs(result$SPREAD) < 3.1),]$FGS_GROUP <- '1'
}
if(length(which(abs(result$SPREAD) >= 3.1 & abs(result$SPREAD) < 8.1)) > 0){
result[which(abs(result$SPREAD) >= 3.1 & abs(result$SPREAD) < 8.1),]$FGS_GROUP <- '2'
}
if(length(which(abs(result$SPREAD) >= 8.1)) > 0){
result[which(abs(result$SPREAD) >= 8.1),]$FGS_GROUP <- '3'
}

result$LINE_HALF.TEAM1<-as.numeric(result$LINE_HALF.TEAM1)
result$HALF_DIFF <- NA
result$underDog.TEAM1 <- (result$HOME_TEAM.TEAM1 == FALSE & result$SPREAD > 0) | (result$HOME_TEAM.TEAM1 == TRUE & result$SPREAD < 0)
under.teams <- which(result$underDog.TEAM1)
favorite.teams <- which(!result$underDog.TEAM1)
result[under.teams,]$HALF_DIFF <- result[under.teams,]$HALF_PTS.TEAM2 - result[under.teams,]$HALF_PTS.TEAM1
result[favorite.teams,]$HALF_DIFF <- result[favorite.teams,]$HALF_PTS.TEAM1 - result[favorite.teams,]$HALF_PTS.TEAM2
result$MWTv2 <- result$LINE_HALF.TEAM1 - (result$LINE.TEAM1 /2)
result$possessions.TEAM1 <- result$HALF_FGA.TEAM1 + (result$HALF_FTA.TEAM1 / 2) + result$HALF_TO.TEAM1 - result$HALF_OREB.TEAM1
result$possessions.TEAM2 <- result$HALF_FGA.TEAM2 + (result$HALF_FTA.TEAM2 / 2) + result$HALF_TO.TEAM2 - result$HALF_OREB.TEAM2
result$possessions.TEAM1.SEASON <- result$SEASON_FGA.TEAM1 + (result$SEASON_FTA.TEAM1 / 2) + result$SEASON_TPG.TEAM1 - result$SEASON_ORPG.TEAM1
result$possessions.TEAM2.SEASON <- result$SEASON_FGA.TEAM2 + (result$SEASON_FTA.TEAM2 / 2) + result$SEASON_TPG.TEAM2 - result$SEASON_ORPG.TEAM2
result$POSSvE <- NA

## Adjust this for Fav and Dog
result[under.teams,]$POSSvE <- ((result[under.teams,]$possessions.TEAM2 + result[under.teams,]$possessions.TEAM1) / 2) - ((result[under.teams,]$possessions.TEAM2.SEASON /
                                2 + result[under.teams,]$possessions.TEAM1.SEASON / 2) / 2)
result[favorite.teams,]$POSSvE <- ((result[favorite.teams,]$possessions.TEAM1 + result[favorite.teams,]$possessions.TEAM2) / 2) - ((result[favorite.teams,]$possessions.TEAM1.SEASON /
                                2 + result[favorite.teams,]$possessions.TEAM2.SEASON / 2) / 2)
result$P100vE <- NA
result$P100.TEAM1 <- result$HALF_PTS.TEAM1 / result$possessions.TEAM1 * 100
result$P100.TEAM1.SEASON <- result$SEASON_PPG.TEAM1 / result$possessions.TEAM1.SEASON * 100
result$P100.TEAM2 <- result$HALF_PTS.TEAM2 / result$possessions.TEAM2 * 100
result$P100.TEAM2.SEASON <- result$SEASON_PPG.TEAM2 / result$possessions.TEAM2.SEASON * 100

result$P100_DIFF <- NA
result[under.teams,]$P100_DIFF <- (result[under.teams,]$P100.TEAM2 - result[under.teams,]$P100.TEAM2.SEASON) - (result[under.teams,]$P100.TEAM1 - result[under.teams,]$P100.TEAM1.SEASON)
result[favorite.teams,]$P100_DIFF <- (result[favorite.teams,]$P100.TEAM1 - result[favorite.teams,]$P100.TEAM1.SEASON) - (result[favorite.teams,]$P100.TEAM2 - result[favorite.teams,]$P100.TEAM2.SEASON)
result[favorite.teams,]$P100vE <- (result[favorite.teams,]$P100.TEAM1 - result[favorite.teams,]$P100.TEAM1.SEASON) + (result[favorite.teams,]$P100.TEAM2 -
                                        result[favorite.teams,]$P100.TEAM2.SEASON)
result[under.teams,]$P100vE <- (result[under.teams,]$P100.TEAM2 - result[under.teams,]$P100.TEAM2.SEASON) + (result[under.teams,]$P100.TEAM1 -
                                        result[under.teams,]$P100.TEAM1.SEASON)


result$prediction<-predict(r,newdata=result)
result$FAV <- ""
result[which(result$underDog.TEAM1),]$FAV <- result[which(result$underDog.TEAM1),]$TEAM2.TEAM2
result[which(!result$underDog.TEAM1),]$FAV <- result[which(!result$underDog.TEAM1),]$TEAM1.TEAM1
result$MWTv3 <- 0

i <- which(result$SPREAD > 0)
result$MWTv3[i] <- result[i,]$SPREAD_HALF.TEAM1 - (result[i,]$SPREAD / 2)

i <- which(result$SPREAD <= 0)
result$MWTv3[i] <- -result[i,]$SPREAD_HALF.TEAM1 + (result[i,]$SPREAD / 2)

## Need to merge nba final data first
##esult <- cbind(result,nbafinal[match(result$GAME_ID, nbafinal$GAME_ID),])
result$secondHalfPts.TEAM1 <- result$FINAL_pts.TEAM1 - result$HALF_PTS.TEAM1
result$secondHalfPts.TEAM2 <- result$FINAL_pts.TEAM2 - result$HALF_PTS.TEAM2
result$secondHalfPtsTotal <- result$secondHalfPts.TEAM1 + result$secondHalfPts.TEAM2
result$Over<-result$secondHalfPtsTotal > result$LINE_HALF.TEAM1
#result <- result[-which(is.na(result$Over)),]

i<-avg.points.last.7[match(result$TEAM.TEAM1, avg.points.last.7$team, 0),]$V1
result$TEAM1.last7.pts <- i
i<-avg.points.last.7[match(result$TEAM.TEAM2, avg.points.last.7$team, 0),]$V1
result$TEAM2.last7.pts <- i

write.csv(result, file="/home/ec2-user/sports2016/NBA/sportsbook.csv", row.names=FALSE)

sendmailV <- Vectorize( sendmail , vectorize.args = "to" )
#emails <- c( "<tanyacash@gmail.com>" , "<malloyc@yahoo.com>", "<sschopen@gmail.com>")
emails <- c("<tanyacash@gmail.com>")

from <- "<tanyacash@gmail.com>"
subject <- "Weekly NBA Data Report - SportsBook"
body <- c(
  "Chris -- see the attached file.",
  mime_part("/home/ec2-user/sports2016/NBA/sportsbook.csv", "WeeklyDataNBA_SB.csv")
)
sendmailV(from, to=emails, subject, body)


