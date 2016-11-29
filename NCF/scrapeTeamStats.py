import time
import urllib2
import re
import random
import datetime
import os
import sys
import sqlite3
import jsonpickle
import pandas as pd
from urlparse import urlparse
from bs4 import BeautifulSoup as bs

db = sqlite3.connect('/home/ec2-user/sports2016/NCF/sports.db')

x=random.randint(3, 10)
time.sleep(x)

week_num = str(14)
#divisions = ['http://espn.go.com/college-football/scoreboard/_/year/2015/seasontype/2/week/' + week_num,
divisions = ['http://espn.go.com/college-football/scoreboard/_/group/80/year/2016/seasontype/2/week/' + week_num]
#'http://espn.go.com/college-football/scoreboard/_/group/1/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/51/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/151/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/4/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/5/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/12/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/18/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/15/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/17/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/9/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/8/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/37/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/81/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/20/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/40/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/48/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/32/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/22/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/24/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/21/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/25/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/26/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/27/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/28/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/31/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/29/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/30/year/2015/seasontype/2/week/' + week_num,
#'http://espn.go.com/college-football/scoreboard/_/group/35/year/2015/seasontype/2/week/' +  week_num]

for division in divisions:
    final_ids = []
    url = urllib2.urlopen(division)
    soup = bs(url.read())

    data=re.search('window.espn.scoreboardData.*{(.*)};</script>', str(soup)).group(0)
    jsondata=re.search('({.*});window', data).group(1)
    j=jsonpickle.decode(jsondata)
    games=j['events']
    status = [game['status'] for game in games]
    final = [s['type']['shortDetail'] for s in status]
    index = [i for i, j in enumerate(final) if j == 'Final']
    ids = [game['id'] for game in games]
    final_ids = [j for k, j in enumerate(ids) if k in index]

    if(len(final_ids) == 0):
        print "No Final Box Scores yet."
    else:
        for i in range(0, len(final_ids)):
            print final_ids[i]
            x=random.randint(3, 5) 
            time.sleep(x)
            espn1 = 'http://espn.go.com/college-football/game?gameId=' + final_ids[i]
            url = urllib2.urlopen(espn1)
            soup = bs(url.read())
            game_date=soup.findAll("span", {"data-date": True})[0]['data-date']
            t=time.strptime(game_date, "%Y-%m-%dT%H:%MZ")
            gdate=time.strftime('%m/%d/%Y %H:%M', t)
            x=random.randint(3, 5)
            time.sleep(x)
            espn = 'http://espn.go.com/college-football/matchup?gameId=' + final_ids[i]
            url = urllib2.urlopen(espn)
            soup = bs(url.read())
            boxscore = soup.find('table', {'class':'mod-data'})
            team1 = soup.findAll('span', {'class':'abbrev'})[0].text
            team2 = soup.findAll('span', {'class':'abbrev'})[1].text
            espn = 'http://espn.go.com/college-football/boxscore?gameId=' + final_ids[i]
            url = urllib2.urlopen(espn)
            soup = bs(url.read())
            stat_tables = soup.findAll('table', {'class':'mod-data'})
            stats = [s.findAll('tr', {'class':'highlight'}) for s in stat_tables]
            if(len(stats[0]) != 0):
                if(len(stats[0]) == 1):
                    team1_passing = [h.text for h in stats[0][0]]
                    if(len(team1_passing) == 6):
                        team1_passing.append('-')
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsPassing(team, the_date, comp_att,yards,avg_yards,td,ints,qbr) VALUES(?,?,?,?,?,?,?,?)''', (team1, gdate,team1_passing[1], int(team1_passing[2]),float(team1_passing[3]),int(team1_passing[4]),int(team1_passing[5]), team1_passing[6]))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass 
                if(len(stats[1]) == 1):
                    team2_passing = [h.text for h in stats[1][0]] 
                    if(len(team2_passing) == 6):
                        team2_passing.append('-')
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsPassing(team, the_date, comp_att,yards,avg_yards,td,ints,qbr) VALUES(?,?,?,?,?,?,?,?)''', (team2, gdate,team2_passing[1], int(team2_passing[2]),float(team2_passing[3]),int(team2_passing[4]),int(team2_passing[5]), team2_passing[6]))
       	       	            db.commit()	
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[2]) == 1):
                    team1_rushing = [h.text for h in stats[2][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsRushing(team, the_date, carries,yards,avg_yards,td,long_yards) VALUES(?,?,?,?,?,?,?)''', (team1, gdate,int(team1_rushing[1]),int(team1_rushing[2]),float(team1_rushing[3]),int(team1_rushing[4]),int(team1_rushing[5])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[3]) == 1):
                    team2_rushing = [h.text for h in stats[3][0]]
     	       	    with db:
                        try: 
                            db.execute('''INSERT INTO teamStatsRushing(team, the_date, carries,yards,avg_yards,td,long_yards) VALUES(?,?,?,?,?,?,?)''', (team2, gdate,int(team2_rushing[1]),int(team2_rushing[2]),float(team2_rushing[3]),int(team2_rushing[4]),int(team2_rushing[5])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[4]) == 1):
                    team1_receiving = [h.text for h in stats[4][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsReceiving(team, the_date, rec,yards,avg_yards,td,long_yards) VALUES(?,?,?,?,?,?,?)''', (team1, gdate,int(team1_receiving[1]),int(team1_receiving[2]),float(team1_receiving[3]),int(team1_receiving[4]),int(team1_receiving[5])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[5]) == 1):
                    team2_receiving = [h.text for h in stats[5][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsReceiving(team, the_date, rec,yards,avg_yards,td,long_yards) VALUES(?,?,?,?,?,?,?)''', (team2, gdate,int(team2_receiving[1]),int(team2_receiving[2]),float(team2_receiving[3]),int(team2_receiving[4]),int(team2_receiving[5])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[6]) == 1):
                    team1_ints = [h.text for h in stats[6][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsInts(team, the_date, ints,yards,td) VALUES(?,?,?,?,?)''', (team1, gdate,int(team1_ints[1]),int(team1_ints[2]),int(team1_ints[3])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[7]) == 1):
                    team2_ints = [h.text for h in stats[7][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsInts(team, the_date, ints,yards,td) VALUES(?,?,?,?,?)''', (team2, gdate,int(team2_ints[1]),int(team2_ints[2]),int(team2_ints[3])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[8]) == 1):
                    team1_kick_returns = [h.text for h in stats[8][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsKickReturns(team, the_date, no, yards, avg_yards, long_yards, td) VALUES(?,?,?,?,?,?,?)''', (team1, gdate,int(team1_kick_returns[1]),int(team1_kick_returns[2]),float(team1_kick_returns[3]),int(team1_kick_returns[4]),int(team1_kick_returns[5])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[9]) == 1):
                    team2_kick_returns = [h.text for h in stats[9][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsKickReturns(team, the_date, no, yards, avg_yards, long_yards, td) VALUES(?,?,?,?,?,?,?)''', (team2, gdate,int(team2_kick_returns[1]),int(team2_kick_returns[2]),float(team2_kick_returns[3]),int(team2_kick_returns[4]),int(team2_kick_returns[5])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[10]) == 1):
                    team1_punt_returns = [h.text for h in stats[10][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsPuntReturns(team, the_date, no, yards, avg_yards, long_yards, td) VALUES(?,?,?,?,?,?,?)''', (team1, gdate,int(team1_punt_returns[1]),int(team1_punt_returns[2]),float(team1_punt_returns[3]),int(team1_punt_returns[4]), int(team1_punt_returns[5])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[11]) == 1):
                    team2_punt_returns = [h.text for h in stats[11][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsPuntReturns(team, the_date, no, yards, avg_yards, long_yards, td) VALUES(?,?,?,?,?,?,?)''', (team2, gdate,int(team2_punt_returns[1]),int(team2_punt_returns[2]),float(team2_punt_returns[3]),int(team2_punt_returns[4]),int(team2_punt_returns[5])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[12]) == 1):
                    team1_kicking = [h.text for h in stats[12][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsKicking(team, the_date, fg, pct, long_yards, xp,pts) VALUES(?,?,?,?,?,?,?)''', (team1, gdate,team1_kicking[1], float(team1_kicking[2]),int(team1_kicking[3]),team1_kicking[4],int(team1_kicking[5])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[13]) == 1):
                    team2_kicking = [h.text for h in stats[13][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsKicking(team, the_date, fg, pct, long_yards, xp,pts) VALUES(?,?,?,?,?,?,?)''', (team2, gdate,team2_kicking[1], float(team2_kicking[2]), int(team2_kicking[3]), team2_kicking[4], int(team2_kicking[5])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[14]) == 1):
                    team1_punting = [h.text for h in stats[14][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsPunting(team, the_date, no, yards, avg_yards,tb,in_20,long_yards) VALUES(?,?,?,?,?,?,?,?)''', (team1, gdate,int(team1_punting[1]), int(team1_punting[2]),team1_punting[3],int(team1_punting[4]), int(team1_punting[5]), int(team1_punting[6])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass
                if(len(stats[15]) == 1):
                    team2_punting = [h.text for h in stats[15][0]]
                    with db:
                        try:
                            db.execute('''INSERT INTO teamStatsPunting(team, the_date, no, yards, avg_yards,tb,in_20,long_yards) VALUES(?,?,?,?,?,?,?,?)''', (team2, gdate,int(team2_punting[1]), int(team2_punting[2]),team2_punting[3],int(team2_punting[4]), int(team2_punting[5]), int(team2_punting[6])))
                            db.commit()
                        except:
                            print sys.exc_info()[0]
                            pass

db.close()
