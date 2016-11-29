#library(data.table)
library(RSQLite)
library(sendmailR)
#library(plyr)
library(dplyr)
library(data.table)

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

halflines <- lDataFrames[[which(tables == "NCFSBHalfLines")]]
lines <- lDataFrames[[which(tables == "NCFSBLines")]]
lookup <- lDataFrames[[which(tables == "NCFSBTeamLookup")]]
halfbox <- lDataFrames[[which(tables == "halfBoxScore")]] 
finalbox <- lDataFrames[[which(tables == "finalBoxScore")]] 
games <- lDataFrames[[which(tables == "games")]]

## Add ESPN abbreviations to line data
halflines$away_espn<-lookup[match(halflines$away_team, lookup$sb_team),]$espn_abbr
halflines$home_espn<-lookup[match(halflines$home_team, lookup$sb_team),]$espn_abbr
lines$away_espn<-lookup[match(lines$away_team, lookup$sb_team),]$espn_abbr
lines$home_espn<-lookup[match(lines$home_team, lookup$sb_team),]$espn_abbr
halflines <- halflines[c("away_espn", "home_espn", "line", "spread", "game_date")]
lines <- lines[c("away_espn", "home_espn", "line", "spread", "game_date")]

## Join games and half lines data
games$game_time <- games$game_date
games$game_date<-substr(games$game_date,0,10)
games$key <- paste(games$team1, games$team2, games$game_date)
halflines$key <- paste(halflines$away_espn, halflines$home_espn, halflines$game_date)
games <- merge(halflines, games)

lines$key <- paste(lines$away_espn, lines$home_espn, lines$game_date)
games <- merge(lines, games, by="key")

#halfbox <- merge(games, halfbox)[c("game_id", "game_date.x", "away_espn.x", "home_espn.x", "team", "line.x", "spread.x", "line.y", "spread.y","first_downs", 
#            "third_downs", "fourth_downs", "total_yards", "passing", "comp_att", "yards_per_pass", "rushing", "rushing_attempts", 
#            "yards_per_rush","penalties","turnovers", "fumbles_lost","ints_thrown", "possession", "score")]
halfbox <- merge(games, halfbox)[c("game_id", "game_time", "away_espn.x", "home_espn.x", "team", "line.x", "spread.x", "total_yards", "score", "line.y", 
					"spread.y")]
halfbox <- halfbox[order(halfbox$game_id),]
halfbox$tempteam <- ""
halfbox$tempteam[which(halfbox$team == halfbox$away_espn)] <- "team1"
halfbox$tempteam[which(halfbox$team != halfbox$away_espn)] <- "team2"

## calculate running first half season averages and merge with all data
halfbox$game_time<-as.POSIXct(halfbox$game_time, format='%m/%d/%Y %H:%M')
halfbox<-halfbox[order(halfbox$game_time),]

## Split out the meaures with '-' in them into 2 numeric variables
#halfbox<-cbind(halfbox, do.call('rbind', strsplit(halfbox$third_downs, "-")))
#colnames(halfbox)[27:28] <- c("third_downs", "third_down_att")
#halfbox<-cbind(halfbox, do.call('rbind', strsplit(halfbox$fourth_downs, "-")))
#colnames(halfbox)[29:30] <- c("fourth_downs", "fourth_down_att")
#halfbox<-cbind(halfbox, do.call('rbind', strsplit(halfbox$penalties, "-")))
#colnames(halfbox)[31:32] <- c("penalties", "penalty_yards")
#halfbox[,27:32] <- apply(halfbox[,27:32], 2, as.numeric)
#halfbox <- halfbox[,c(-11,-12,-15,-20)]

## Split out the meaures with '-' in them into 2 numeric variables
#finalbox<-cbind(finalbox, do.call('rbind', strsplit(finalbox$third_downs, "-")))
#colnames(finalbox)[19:20] <- c("third_downs", "third_down_att")
#finalbox<-cbind(finalbox, do.call('rbind', strsplit(finalbox$fourth_downs, "-")))
#colnames(finalbox)[21:22] <- c("fourth_downs", "fourth_down_att")
#finalbox<-cbind(finalbox, do.call('rbind', strsplit(finalbox$penalties, "-")))
#colnames(finalbox)[23:24] <- c("penalties", "penalty_yards")
#finalbox[,19:24] <- apply(finalbox[,19:24], 2, as.numeric)
#finalbox <- finalbox[,c(-4, -5, -8, -13)]

## Calculate 2nd half stats using halfbox and finalbox
both.halves <- merge(halfbox, finalbox, by=c("game_id", "team"))
#both.halves$third_downs_2H <- both.halves$third_downs.y - both.halves$third_downs.x
#both.halves$third_down_att_2H <- both.halves$third_down_att.y - both.halves$third_down_att.x
#both.halves$fourth_downs_2H <- both.halves$fourth_downs.y - both.halves$fourth_downs.x
#both.halves$fourth_down_att_2H <- both.halves$fourth_down_att.y - both.halves$fourth_down_att.x
#both.halves$penalties_2H <- both.halves$penalties.y - both.halves$penalties.x
#both.halves$penalty_yards_2H <- both.halves$penalty_yards.y - both.halves$penalty_yards.x
both.halves$total_yards_2H <- both.halves$total_yards.y - both.halves$total_yards.x
#both.halves$pass_yards_2H <- both.halves$passing.y - both.halves$passing.x
both.halves$points_2H <- both.halves$score.y - both.halves$score.x
#both.halves$turnovers_2H <- both.halves$turnovers.y - both.halves$turnovers.x
#both.halves$points_2H <- both.halves$score.y - both.halves$score.x

## Calculate first half season totals and averages
#halfbox<-data.frame(halfbox %>% group_by(team) %>% mutate(count = sequence(n())))
#dt <- data.table(halfbox)
#dt <- dt[, season_1H_third_down_total:=cumsum(third_downs), by = "team"]
#dt <- dt[, season_1H_third_down_att_total:=cumsum(third_down_att), by = "team"]
#dt <- dt[, season_1H_third_down_conv:=season_1H_third_down_total /season_1H_third_down_att_total, by = "team"]
#dt <- dt[, season_1H_fourth_down_total:=cumsum(fourth_downs), by = "team"]
#dt <- dt[, season_1H_yards_total:=cumsum(total_yards), by = "team"]
#dt <- dt[, season_1H_pass_yards_total:=cumsum(passing), by = "team"]
#dt <- dt[, season_1H_score_total:=cumsum(score), by="team"]
#dt <- dt[, season_1H_fourth_down_avg:=season_1H_fourth_down_total / count, by = "team"]
#dt <- dt[, season_1H_yards_avg:=season_1H_yards_total / count, by = "team"]
#dt <- dt[, season_1H_pass_yards_avg:=season_1H_pass_yards_total / count, by = "team"]
#dt <- dt[, season_1H_penalty_yards_total:=cumsum(penalty_yards), by = "team"]
#dt <- dt[, season_1H_penalty_yards_avg:=season_1H_penalty_yards_total / count, by = "team"]
#dt <- dt[, season_1H_turnovers_total:=cumsum(turnovers), by = "team"]
#dt <- dt[, season_1H_turnovers_avg:=season_1H_turnovers_total / count, by = "team"]
#dt <- dt[, season_1H_avg_points:=season_1H_score_total / count, by="team"]
#dt <- dt[, season_1H_avg_points_per_100_yards:=(season_1H_score_total / season_1H_yards_total) * 100, by="team"]

## Lag every season variable by 1 game so stats are going into the game
#nm1<-grep("season_1H", colnames(dt), value=TRUE)
#nm2 <- paste("lag", nm1, sep=".")
#dt[, (nm2):=lapply(.SD, function(x) c(NA, x[-.N])), by=team, .SDcols=nm1]

#halfbox <- data.frame(dt)
#halfbox <- halfbox[,-grep("^season", colnames(halfbox))]

#secondhalf.box <- both.halves[,c("game_id", "game_date.x", "team", "third_downs_2H", "third_down_att_2H", "fourth_downs_2H", "fourth_down_att_2H", 
#"penalties_2H", "penalty_yards_2H", "total_yards_2H", "pass_yards_2H", "turnovers_2H", "score.y","points_2H")]
secondhalf.box <- both.halves[,c("game_id", "game_time", "team", "total_yards_2H", "score.y", "points_2H")]

## calculate running second half season averages and merge with all data
#secondhalf.box$game_date.x<-as.Date(secondhalf.box$game_date.x, format='%m/%d/%Y')
#secondhalf.box<-secondhalf.box[order(secondhalf.box$game_date),]

#secondhalf.box<-data.frame(secondhalf.box %>% group_by(team) %>% mutate(count = sequence(n())))
#dt <- data.table(secondhalf.box)
#dt <- dt[, season_2H_third_down_total:=cumsum(third_downs_2H), by = "team"]
#dt <- dt[, season_2H_third_down_att_total:=cumsum(third_down_att_2H), by = "team"]
#dt <- dt[, season_2H_third_down_conv:=season_2H_third_down_total /season_2H_third_down_att_total, by = "team"]
#dt <- dt[, season_2H_fourth_down_total:=cumsum(fourth_downs_2H), by = "team"]
#dt <- dt[, season_2H_yards_total:=cumsum(total_yards_2H), by = "team"]
#dt <- dt[, season_2H_pass_yards_total:=cumsum(pass_yards_2H), by = "team"]
#dt <- dt[, season_2H_score_total:=cumsum(points_2H), by="team"]
#dt <- dt[, season_2H_fourth_down_avg:=season_2H_fourth_down_total / count, by = "team"]
#dt <- dt[, season_2H_yards_avg:=season_2H_yards_total / count, by = "team"]
#dt <- dt[, season_2H_pass_yards_avg:=season_2H_pass_yards_total / count, by = "team"]
#dt <- dt[, season_2H_penalty_yards_total:=cumsum(penalty_yards_2H), by = "team"]
#dt <- dt[, season_2H_penalty_yards_avg:=season_2H_penalty_yards_total / count, by = "team"]
#dt <- dt[, season_2H_turnovers_total:=cumsum(turnovers_2H), by = "team"]
#dt <- dt[, season_2H_turnovers_avg:=season_2H_turnovers_total / count, by = "team"]
#dt <- dt[, season_2H_avg_score:=season_2H_score_total / count, by="team"]
#dt <- dt[, season_2H_avg_points_per_100_yards:=(season_2H_score_total / season_2H_yards_total) * 100, by="team"]


## Lag every season variable by 1 game so stats are going into the game
#nm1<-grep("season_2H", colnames(dt), value=TRUE)
#nm2 <- paste("lag", nm1, sep=".")
#dt[, (nm2):=lapply(.SD, function(x) c(NA, x[-.N])), by=team, .SDcols=nm1]

#secondhalf.box <- data.frame(dt)
#secondhalf.box <- secondhalf.box[,-grep("^season", colnames(secondhalf.box))]

secondhalf.box <- merge(games, secondhalf.box, by="game_id")
#secondhalf.box <- secondhalf.box[,c(-2,-8:-10, -13:-15)]
#secondhalf.box <- secondhalf.box[order(secondhalf.box$game_id),]
secondhalf.box$tempteam <- ""
secondhalf.box$tempteam[which(secondhalf.box$team == secondhalf.box$away_espn.x)] <- "team1"
secondhalf.box$tempteam[which(secondhalf.box$team != secondhalf.box$away_espn.x)] <- "team2"


wide<-reshape(halfbox[,c(-2:-4)], direction = "wide", idvar="game_id", timevar="tempteam")
widefinal<-reshape(secondhalf.box[,c(-2:-3)], direction = "wide", idvar="game_id", timevar="tempteam")

#wide <- subset(wide, select = -c(line.x.team2, spread.x.team2, line.y.team2,spread.y.team2 ))
widefinal<-subset(widefinal, select=-c(line.x.team2, spread.x.team2, line.y.team2, spread.y.team2))

all <- merge(wide, widefinal, by="game_id")
#colnames(all) <- gsub("lag\\.", "", colnames(all))
#colnames(all)[c(38:42)] <- c('team1', 'full_line', 'full_spread.team1', 'half_line', 'half_spread.team1')

#all <- all[,c("game_id", "team1.team1", "team2.team1", "spread.x.team1.x", "line.x.team1.x", "total_yards.team1", "total_yards.team2", "score.team1", 
#		"score.team2", "line.y.team1.x", "spread.y.team1.x","total_yards_2H.team1", "total_yards_2H.team2", "score.y.team1", 
#

all <- all[,c("game_id", "game_time.x.team1", "team.team1.x", "team2.team2", "spread.x.team1.x", "line.x.team1.x", "total_yards.team1", "total_yards.team2", 
		"score.team1", "score.team2", "line.y.team1.x", "spread.y.team1.x", "total_yards_2H.team1", "total_yards_2H.team2", "score.y.team1", "score.y.team2")]			
colnames(all) <- c("game_id", "game_date", "team1", "team2", "spread", "line", "total_yards.team1", "total_yards.team2", "score.team1", "score.team2", "half.line", 
			"half.spread", "total_yards_2H.team1", "total_yards_2H.team2", "score.final.team1", "score.final.team2")
#all$first.half.points100Yards.team1 <- all$score.team1 / all$total_yards.team1 * 100
#all$first.half.points100Yards.team2 <- all$score.team2 / all$total_yards.team2 * 100

all <- all[order(all$game_date, decreasing=TRUE),]
all$spread <- as.numeric(gsub("\\+", "", all$spread)) * -1
all$half.spread <- as.numeric(gsub("\\+", "", all$half.spread)) * -1

library(googlesheets)
library(dplyr)

gs_auth(token = '/home/ec2-user/sports2016/NCF/ttt.rds')

ncf <- gs_key('1W0fTkdFCUNRRdGjXIOOmjeAyJbUUH2o6k0d6JgDcBCQ', visibility = 'private')

#n_rows <- nrow(the_data)
#n_cols <- ncol(the_data)

#empty_df <- data.frame(mat.or.vec(n_rows, n_cols+1))
#empty_df[empty_df==0] <- ''

#ncf <- ncf %>%
#	gs_edit_cells(input = empty_df)

gs_edit_cells(ncf, ws='postgame', input=colnames(all), byrow=TRUE, anchor="A1")

#ncf <- ncf %>%
#       gs_edit_cells(input = empty_df[1,])


gs_edit_cells(ncf, ws='postgame', input = all, anchor="A2", col_names=FALSE, trim=TRUE)








write.csv(all, file="/home/ec2-user/sports2016/NCF/testfile.csv", row.names=FALSE)

sendmailV <- Vectorize( sendmail , vectorize.args = "to" )
#emails <- c( "<tanyacash@gmail.com>" , "<malloyc@yahoo.com>", "<sschopen@gmail.com>")
emails <- c("<tanyacash@gmail.com>")

from <- "<tanyacash@gmail.com>"
subject <- "Weekly NCF Data Report"
body <- c(
  "Chris -- see the attached file.",
  mime_part("/home/ec2-user/sports2015/NCF/testfile.csv", "WeeklyData.csv")
)
#sendmailV(from, to=emails, subject, body)



dbDisconnect(con)
