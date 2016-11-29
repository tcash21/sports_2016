CREATE TABLE "halfBoxScore"(
game_id INT NOT NULL,
team CHAR(5) NOT NULL,
first_downs INT NOT NULL,
third_downs TEXT NOT NULL,
fourth_downs TEXT NOT NULL,
total_yards INT NOT NULL,
passing INT NOT NULL,
comp_att TEXT NOT NULL,
yards_per_pass NUMERIC NOT NULL,
rushing INT NOT NULL,
rushing_attempts INT NOT NULL,
yards_per_rush NUMERIC NOT NULL,
penalties TEXT NOT NULL,
turnovers INT NOT NULL,
fumbles_lost INT NOT NULL,
ints_thrown INT NOT NULL,
possession TEXT NOT NULL,
score INT NOT NULL,
PRIMARY KEY (game_id, team)
);
CREATE TABLE "finalBoxScore"(
game_id INT NOT NULL,
team CHAR(5) NOT NULL,
first_downs INT NOT NULL,
third_downs TEXT NOT NULL,
fourth_downs TEXT NOT NULL,
total_yards INT NOT NULL,
passing INT NOT NULL,
comp_att TEXT NOT NULL,
yards_per_pass NUMERIC NOT NULL,
rushing INT NOT NULL,
rushing_attempts INT NOT NULL,
yards_per_rush NUMERIC NOT NULL,
penalties TEXT NOT NULL,
turnovers INT NOT NULL,
fumbles_lost INT NOT NULL,
ints_thrown INT NOT NULL,
possession TEXT NOT NULL,
score INT NOT NULL,
PRIMARY KEY (game_id, team)
);
CREATE TABLE "halfBoxScoreTest"(
game_id INT NOT NULL,
team CHAR(5) NOT NULL,
first_downs INT NOT NULL,
third_downs TEXT NOT NULL,
fourth_downs TEXT NOT NULL,
total_yards INT NOT NULL,
passing INT NOT NULL,
comp_att TEXT NOT NULL,
yards_per_pass NUMERIC NOT NULL,
rushing INT NOT NULL,
rushing_attempts INT NOT NULL,
yards_per_rush NUMERIC NOT NULL,
penalties TEXT NOT NULL,
turnovers INT NOT NULL,
fumbles_lost INT NOT NULL,
ints_thrown INT NOT NULL,
possession TEXT NOT NULL,
score INT NOT NULL,
PRIMARY KEY (game_id, team)
);
CREATE TABLE games(
        game_id INT PRIMARY KEY NOT NULL,
        team1 CHAR(5) NOT NULL,
        team2 CHAR(5) NOT NULL,
        game_date TEXT NOT NULL
);
CREATE TABLE NCFSBLines (  away_team CHAR(40) NOT NULL,
        home_team CHAR(40) NOT NULL,
        line CHAR(10) NOT NULL,
        spread CHAR(10) NOT NULL,
        game_date TEXT NOT NULL,
        game_time TEXT NOT NULL,
        PRIMARY KEY (away_team, home_team, game_date, line, spread)
);
CREATE TABLE NCFSBHalfLines (away_team CHAR(40) NOT NULL,
        home_team CHAR(40) NOT NULL,
        line CHAR(10) NOT NULL,
        spread CHAR(10) NOT NULL,
        game_date TEXT NOT NULL,
        game_time TEXT NOT NULL,
        PRIMARY KEY (away_team, home_team, game_date, line, spread)
);
CREATE TABLE teamStatsPassing (
    team CHAR(5) NOT NULL,
    the_date TEXT NOT NULL,
    comp_att TEXT,
    yards INT,
    avg_yards NUMERIC,
    td INT,
    ints INT,
    qbr TEXT,
    PRIMARY KEY (team, the_date)
);
CREATE TABLE teamStatsRushing (
    team CHAR(5) NOT NULL,
    the_date TEXT NOT NULL,
    carries INT,
    yards INT,
    avg_yards NUMERIC,
    td INT,
    long_yards INT,
    PRIMARY KEY (team, the_date)
);
CREATE TABLE teamStatsReceiving (
    team CHAR(5) NOT NULL,
    the_date TEXT NOT NULL,
    rec INT,
    yards INT,
    avg_yards NUMERIC,
    td INT,
    long_yards INT,
    PRIMARY KEY (team, the_date)
);
CREATE TABLE teamStatsInts (
    team CHAR(5) NOT NULL,
    the_date TEXT NOT NULL,
    ints INT,
    yards INT,
    td INT,
    PRIMARY KEY (team, the_date)
);
CREATE TABLE teamStatsKickReturns (
    team CHAR(5) NOT NULL,
    the_date TEXT NOT NULL,
    no INT,
    yards INT,
    avg_yards NUMERIC,
    long_yards INT,
    td INT,
    PRIMARY KEY (team, the_date)
);
CREATE TABLE teamStatsPuntReturns (
    team CHAR(5) NOT NULL,
    the_date TEXT NOT NULL,
    no INT,
    yards INT,
    avg_yards NUMERIC,
    long_yards INT,
    td INT,
    PRIMARY KEY (team, the_date)
);
CREATE TABLE teamStatsKicking (
    team CHAR(5) NOT NULL,
    the_date TEXT NOT NULL,
    fg TEXT,
    pct NUMERIC,
    long_yards INT,
    xp TEXT,
    pts INT,
    PRIMARY KEY (team, the_date)
);
CREATE TABLE teamStatsPunting (
    team CHAR(5) NOT NULL,
    the_date TEXT NOT NULL,
    no INT,
    yards INT,
    avg_yards NUMERIC,
    tb INT,
    in_20 INT,
    long_yards INT,
    PRIMARY KEY (team, the_date)
);
CREATE TABLE NCFSBTeamLookup (sb_team CHAR(50) NOT NULL, espn_abbr CHAR(5), PRIMARY KEY (sb_team));
