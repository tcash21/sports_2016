library(randomForest)
library(data.table)
library(RSQLite)
#library(plyr)
library(dplyr)

setwd("/home/ec2-user/sports2016/NCF/")

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
games$game_date <- substr(games$game_date,0,10)
games$key <- paste(games$team1, games$team2, games$game_date)
halflines$key <- paste(halflines$away_espn, halflines$home_espn, halflines$game_date)
games <- merge(halflines, games)

lines$key <- paste(lines$away_espn, lines$home_espn, lines$game_date)
games <- merge(lines, games, by="key")

#halfbox <- merge(games, halfbox)[c("game_id", "game_date.x", "away_espn.x", "home_espn.x", "team", "line.x", "spread", "line.y", "first_downs",
#            "third_downs", "fourth_downs", "total_yards", "passing", "comp_att", "yards_per_pass", "rushing", "rushing_attempts",
#            "yards_per_rush","penalties","turnovers", "fumbles_lost","ints_thrown", "possession", "score")]
halfbox <- merge(games, halfbox)[c("game_id", "game_date.x", "away_espn.x", "home_espn.x", "team", "line.x", "spread.x", "total_yards", "score", "line.y",
                                        "spread.y", "game_time")]

halfbox <- halfbox[order(halfbox$game_id),]
halfbox$tempteam <- ""
halfbox$tempteam[which(halfbox$team == halfbox$away_espn)] <- "team1"
halfbox$tempteam[which(halfbox$team != halfbox$away_espn)] <- "team2"

halfbox$spread.x <- as.numeric(gsub("\\+", "", halfbox$spread.x))
halfbox$spread.y <- as.numeric(gsub("\\+", "", halfbox$spread.y))


wide<-reshape(halfbox[,c(-3:-4)], direction = "wide", idvar="game_id", timevar="tempteam")
wide$first.half.points <- wide$score.team1 + wide$score.team2
#colnames(wide)[6] <- "half.line"
#wide$half.line <- as.numeric(wide$half.line)
wide$score.diff<-wide$score.team1 - wide$score.team2
#wide$spread.team1 <- as.numeric(gsub("\\+", "", wide$spread.team1)) * -1
#wide$abs.score.diff <- abs(wide$score.diff)
#wide$abs.spread <- abs(wide$spread.team1)
wide$team1.favorite <- wide$spread.x.team1 < 0

load("rf.Rdat")

w <- wide

w$favorite.yards <- 0
w$underdog.yards <- 0
w$favorite.yards[which(w$team1.favorite == TRUE)] <- w[which(w$team1.favorite == TRUE),]$total_yards.team1
w$favorite.yards[which(w$team1.favorite == FALSE)] <- w[which(w$team1.favorite == FALSE),]$total_yards.team2

w$underdog.yards[which(w$team1.favorite == FALSE)] <- w[which(w$team1.favorite == FALSE),]$total_yards.team1
w$underdog.yards[which(w$team1.favorite == TRUE)] <- w[which(w$team1.favorite == TRUE),]$total_yards.team2

w$favorite100yds <- 0
w$underdog100yds <- 0

w$first.half.points100Yards.team1 <- w$score.team1 / w$total_yards.team1 * 100
w$first.half.points100Yards.team2 <- w$score.team2 / w$total_yards.team2 * 100


w$favorite100yds[which(w$team1.favorite == TRUE)] <- w[which(w$team1.favorite == TRUE),]$first.half.points100Yards.team1
w$favorite100yds[which(w$team1.favorite == FALSE)] <- w[which(w$team1.favorite == FALSE),]$first.half.points100Yards.team2

w$underdog100yds[which(w$team1.favorite == FALSE)] <- w[which(w$team1.favorite == FALSE),]$first.half.points100Yards.team1
w$underdog100yds[which(w$team1.favorite == TRUE)] <- w[which(w$team1.favorite == TRUE),]$first.half.points100Yards.team2

w$signal1 <- w$favorite100yds > 11
w$signal2 <- w$favorite.yards > 355
w$signal3 <- w$underdog.yards > 295
w$signal4 <- w$underdog100yds > 15

w$signal1 <- as.factor(w$signal1)
w$signal2 <- as.factor(w$signal2)
w$signal3 <- as.factor(w$signal3)
w$signal4 <- as.factor(w$signal4)

w$abn1 <- as.numeric(w$signal1) - 1
w$abn2 <- as.numeric(w$signal2) - 1
w$abn3 <- as.numeric(w$signal3) - 1
w$abn4 <- as.numeric(w$signal4) - 1
w$signals <- w$abn1 + w$abn2 + w$abn3 + w$abn4
w$signals<-as.factor(w$signals)


#w$second.half.score.team1 <- w$score.y.team1  -  w$score.team1
#w$second.half.score.team2 <- w$score.y.team2  -  w$score.team2

#w$half.line<-as.numeric(as.character(w$line.y))
#w$second.half.points <- w$second.half.score.team1 + w$second.half.score.team2
w$first.half.points <- w$score.team1 + w$score.team2

#w$Over <- w$second.half.points > w$half.line
#w <- w[-which(is.na(w$Over)),]
#w$Over<-as.factor(w$Over)

#w$half.line <- as.numeric(as.character(w$line.y))
#w$line.x.team1<-as.numeric(w$line.x.team1)
#w$mwt <- w$score.team1 + w$score.team2 + (w$half.line - w$line.x.team1)
w$team1.favorite <- w$spread.x.team1 > 0
w$favorite.score <- 0
w$underdog.score <- 0

#z<-head(w[,c(1,70,96,35,128:130,13,48 )])
w$favorite.score[which(w$team1.favorite == TRUE)] <- w[which(w$team1.favorite == TRUE),]$score.team1
w$favorite.score[which(w$team1.favorite == FALSE)] <- w[which(w$team1.favorite == FALSE),]$score.team2

w$underdog.score[which(w$team1.favorite == FALSE)] <- w[which(w$team1.favorite == FALSE),]$score.team1
w$underdog.score[which(w$team1.favorite == TRUE)] <- w[which(w$team1.favorite == TRUE),]$score.team2

w$favorite.yards <- 0
w$underdog.yards <- 0
w$favorite.yards[which(w$team1.favorite == TRUE)] <- w[which(w$team1.favorite == TRUE),]$total_yards.team1
w$favorite.yards[which(w$team1.favorite == FALSE)] <- w[which(w$team1.favorite == FALSE),]$total_yards.team2

w$underdog.yards[which(w$team1.favorite == FALSE)] <- w[which(w$team1.favorite == FALSE),]$total_yards.team1
w$underdog.yards[which(w$team1.favorite == TRUE)] <- w[which(w$team1.favorite == TRUE),]$total_yards.team2

w$yards.diff <- w$favorite.yards - w$underdog.yards
w$score.diff <- w$favorite.score - w$underdog.score
w$favorite.trailing <- w$score.diff < 0
#w$mwt <- w$score.team1 + w$score.team2 + w$half.line - as.numeric(w$line.x.team1)

#load("/home/ec2-user/sports2016/NCF/rpart.Rdat")
#w$over.prediction <- predict(r, w)[,2]
x <- w[order(w$game_date.x.team2),c("game_id", "game_time.team1", "team.team1", "team.team2", "score.team1", "score.team2", 
		"total_yards.team1", "total_yards.team2", "line.x.team1", "spread.x.team1", "line.y.team1", "spread.y.team1")]

colnames(x) <- c('game_id', 'game_time', 'team1', 'team2', 'score.team1', 'score.team2', 'total_yards.team1', 'total_yards.team2',
		'line', 'spread', 'half.line', 'half.spread')
x$spread <- x$spread * -1
x$half.spread <- x$half.spread * -1

the_data <- x
x$game_time<-as.POSIXct(x$game_time, format='%m/%d/%Y %H:%M')
the_data <- the_data[order(the_data$game_time, decreasing=TRUE),]

#today_date <- format(Sys.Date(), '%m/%d/%Y')
#the_data <- x[which(x$game_date.x.team2 == today_date),]
#the_data

the_data$line <- as.numeric(the_data$line)
the_data$first.half.points <- the_data$score.team1 + the_data$score.team2
the_data$P1001HC <- ((the_data$score.team1 + the_data$score.team2) / (the_data$total_yards.team1 + the_data$total_yards.team2) ) * 100

the_data$signal1 <- the_data$P1001HC
the_data$signal2 <- ((the_data$line - the_data$first.half.points) *  100) / (the_data$total_yards.team1 + the_data$total_yards.team2)
the_data$signal3 <- (the_data$line - the_data$first.half.points) / (the_data$P1001HC)

#table(the_data$second.half.points > the_data$half.line)

#the_data$over <- as.factor(the_data$second.half.points > the_data$half.line)
#ncf <- ncf[-which(is.na(the_data$half.line)),]

the_data$betUnder <- the_data$P1001HC > 9
the_data$betUnder2 <- the_data$signal2 < 1.5
the_data$betUnder3 <- the_data$signal3 < 1
the_data$betOver <- the_data$P1001HC < 2.3


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

today <- format(Sys.Date(), "%m/%d/%Y")
tomorrow <- format(Sys.Date() + 1, "%m/%d/%Y")
yesterday <- format(Sys.Date() - 1, "%m/%d/%Y")
the_data1 <- the_data[grep(today, the_data$game_time),]
the_data2 <- the_data[grep(tomorrow, the_data$game_time),]
the_data3 <- the_data[grep(yesterday, the_data$game_time),]
the_data <- rbind(the_data1, the_data2, the_data3)
gs_edit_cells(ncf, ws='halftime', input=colnames(the_data), byrow=TRUE, anchor="A1")

#ncf <- ncf %>%
#       gs_edit_cells(input = empty_df[1,])


gs_edit_cells(ncf, ws='halftime', input = the_data, anchor="A2", col_names=FALSE, trim=TRUE)

