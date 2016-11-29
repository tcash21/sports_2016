CREATE TABLE NBASBTeamLookup (sb_team CHAR(50) NOT NULL, espn_abbr CHAR(5), espn                                                                                                                                                             _name CHAR(30), PRIMARY KEY (sb_team));
CREATE TABLE NBASBLines (  away_team CHAR(40) NOT NULL,
        home_team CHAR(40) NOT NULL,
        line CHAR(10) NOT NULL,
        spread CHAR(10) NOT NULL,
        game_date TEXT NOT NULL,
        game_time TEXT NOT NULL,
        PRIMARY KEY (away_team, home_team, game_date, line, spread)
);
CREATE TABLE NBASBHalfLines (away_team CHAR(40) NOT NULL,
        home_team CHAR(40) NOT NULL,
        line CHAR(10) NOT NULL,
        spread CHAR(10) NOT NULL,
        game_date TEXT NOT NULL,
        game_time TEXT NOT NULL,
        PRIMARY KEY (away_team, home_team, game_date, line, spread)
);
CREATE TABLE NBAGames(
        game_id INT PRIMARY KEY NOT NULL,
        team1 CHAR(5) NOT NULL,
        team2 CHAR(5) NOT NULL,
        game_date TEXT NOT NULL,
        game_time TEXT NOT NULL
);
CREATE TABLE NBAseasonstats(
                team CHAR(5) NOT NULL,
                the_date TEXT INT NOT NULL,
                fgm REAL NOT NULL,
                fga REAL NOT NULL,
                fgp REAL NOT NULL,
                tpm REAL NOT NULL,
                tpa REAL NOT NULL,
                tpp REAL NOT NULL,
                ftm REAL NOT NULL,
                fta REAL NOT NULL,
                ftp REAL NOT NULL,
                twopm REAL NOT NULL,
                twopa REAL NOT NULL,
                twopp REAL NOT NULL,
                pps REAL NOT NULL,
                afg REAL NOT NULL,
                PRIMARY KEY (team, the_date));
CREATE TABLE NBAseasontotals(
                team CHAR(5) NOT NULL,
                the_date TEXT INT NOT NULL,
                gp INT NOT NULL,
                ppg REAL NOT NULL,
                orpg REAL NOT NULL,
                defr REAL NOT NULL,
                rpg REAL NOT NULL,
                apg REAL NOT NULL,
                spg REAL NOT NULL,
                bpg REAL NOT NULL,
                tpg REAL NOT NULL,
                fpg REAL NOT NULL,
ato REAL NOT NULL,
PRIMARY KEY(team, the_date));
CREATE TABLE NBAfinalstats(
    game_id INT NOT NULL,
    team CHAR(5) NOT NULL,
    fgma TEXT NOT NULL,
    tpma TEXT NOT NULL,
    ftma TEXT NOT NULL,
    oreb INT NOT NULL,
    dreb INT NOT NULL,
    reb INT NOT NULL,
    ast NUMERIC NOT NULL,
    stl INT NOT NULL,
    blk INT NOT NULL,
    turnovers INT NOT NULL,
    pf INT NOT NULL,
    pts INT NOT NULL,
    timestamp TEXT,
    PRIMARY KEY (game_id, team)
);
CREATE TABLE NBAstats(
    game_id INT NOT NULL,
    team CHAR(5) NOT NULL,
    fgma TEXT NOT NULL,
    tpma TEXT NOT NULL,
    ftma TEXT NOT NULL,
    oreb INT NOT NULL,
    dreb INT NOT NULL,
    reb INT NOT NULL,
    ast NUMERIC NOT NULL,
    stl INT NOT NULL,
    blk INT NOT NULL,
    turnovers INT NOT NULL,
    pf INT NOT NULL,
    pts INT NOT NULL,
    timestamp TEXT NOT NULL
);

