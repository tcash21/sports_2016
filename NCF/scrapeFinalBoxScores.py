import sys
import time
import urllib2
import re
import random
import datetime
import os
import sqlite3
import jsonpickle
import pandas as pd
from urlparse import urlparse
from bs4 import BeautifulSoup as bs
from datetime import datetime
from dateutil import tz
from datetime import date, timedelta

db = sqlite3.connect('/home/ec2-user/sports2016/NCF/sports.db')

x=random.randint(3, 10)
time.sleep(x)


def extractStats(statName):
    team1_stat=boxscore.findAll('tr', {'data-stat-attr': statName})[0].contents[3].contents
    team1_stat = re.sub('\s+', '', team1_stat[0])
    team2_stat=boxscore.findAll('tr', {'data-stat-attr': statName})[0].contents[5].contents
    team2_stat = re.sub('\s+', '', team2_stat[0])
    combined_stats = [team1_stat, team2_stat]
    return(combined_stats)

week_num = str(14)
#divisions = ['http://espn.go.com/college-football/scoreboard/_/year/2015/seasontype/2/week/' + week_num,
divisions = ['http://espn.go.com/college-football/scoreboard/_/group/80/year/2016/seasontype/2/week/' + week_num]
#'http://www.espn.com/college-football/scoreboard/_/year/2016/seasontype/2/week/' + week_num]
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
#'http://espn.go.com/college-football/scoreboard/_/group/30/year/2015/seasontype/2/week/' + week_num]
#'http://espn.go.com/college-football/scoreboard/_/group/35/year/2015/seasontype/2/week/' +  week_num]

cur = db.execute("SELECT game_id from finalBoxScore")
game_ids = cur.fetchall()
game_ids = [str(g[0]) for g in game_ids]

for division in divisions:
    final_ids = []
    url = urllib2.urlopen(division)
    soup = bs(url.read())

    data=re.search('window.espn.scoreboardData.*{(.*)};</script>', str(soup)).group(0)
    jsondata=re.search('({.*});window', data).group(1)
    j=jsonpickle.decode(jsondata)
    games=j['events']
    status = [game['status'] for game in games]
    half = [s['type']['shortDetail'] for s in status]
    index = [i for i, j in enumerate(half) if re.match(".*Final.*", j)]
    ids = [game['id'] for game in games]
    final_ids = [j for k, j in enumerate(ids) if k in index]
    final_ids = list(set(final_ids) - set(game_ids))
    print len(final_ids)
    if(len(final_ids) == 0):
        print "No Final Box Scores."
    else:
        for i in range(0, len(final_ids)):  
            print final_ids[i]
            x=random.randint(3, 5)
            time.sleep(x)
            espn1 = 'http://espn.go.com/college-football/game?gameId=' + final_ids[i]
            url = urllib2.urlopen(espn1)
            soup = bs(url.read())
            game_date=soup.findAll("span", {"data-date": True})[0]['data-date']
            from_zone = tz.gettz('UTC')
            to_zone = tz.gettz('America/New_York')
            t=time.strptime(game_date, "%Y-%m-%dT%H:%MZ")
            gdate=time.strftime('%m/%d/%Y %H:%M', t)
            utc=datetime.strptime(gdate, '%m/%d/%Y %H:%M')
            utc=utc.replace(tzinfo=from_zone)
            est_date = utc.astimezone(to_zone)
            gdate = est_date.strftime('%m/%d/%Y %H:%M')
            score1 = soup.findAll('div', {'class':'score icon-font-after'})[0].text
            score2 = soup.findAll('div', {'class':'score icon-font-before'})[0].text
            x=random.randint(3, 5)
            time.sleep(x)
            espn = 'http://espn.go.com/college-football/matchup?gameId=' + final_ids[i]
            url = urllib2.urlopen(espn)
            soup = bs(url.read())
            boxscore = soup.find('table', {'class':'mod-data'})
            team1 = soup.findAll('span', {'class':'abbrev'})[0].text
            team2 = soup.findAll('span', {'class':'abbrev'})[1].text
            try:
                with db:
                    try:
                        firstDowns = extractStats('firstDowns')
                        thirdDowns = extractStats('thirdDownEff')
                        fourthDowns = extractStats('fourthDownEff')
                        totalYards = extractStats('totalYards')
                        passing = extractStats('netPassingYards')
                        completionAtt = extractStats('completionAttempts')
                        ypp = extractStats('yardsPerPass')
                        ints = extractStats('interceptions')
                        rushingYards = extractStats('rushingYards')
                        rushingAtt = extractStats('rushingAttempts')
                        yardsPerRushAttempt = extractStats('yardsPerRushAttempt')
                        totalPenaltiesYards = extractStats('totalPenaltiesYards')
                        turnovers = extractStats('turnovers')
                        fumblesLost = extractStats('fumblesLost')
                        try:
                            possessionTime = extractStats('possessionTime')
                        except:
                            possessionTime = ['-','-']
                            pass
                    except:
                        print sys.exc_info()[0]
                        pass
                    try:
                        with db:
                            db.execute('''INSERT INTO finalBoxScore(game_id, team, first_downs, third_downs, fourth_downs, total_yards, passing, comp_att, 
                                                yards_per_pass, rushing, rushing_attempts, yards_per_rush, penalties, turnovers, fumbles_lost, ints_thrown,
                                                possession, score ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (final_ids[i], team1, int(firstDowns[0]), thirdDowns[0],fourthDowns[0],int(totalYards[0]),int(passing[0]),completionAtt[0],float(ypp[0]),int(rushingYards[0]),int(rushingAtt[0]),float(yardsPerRushAttempt[0]),totalPenaltiesYards[0],int(turnovers[0]),int(fumblesLost[0]),int(ints[0]),possessionTime[0], score1))
                            db.commit()
                    except:
                        print sys.exc_info()[0]
                        pass
                    try:
                        with db:
                            db.execute('''INSERT INTO finalBoxScore(game_id, team, first_downs, third_downs, fourth_downs, total_yards, passing, comp_att, 
                                                yards_per_pass, rushing, rushing_attempts, yards_per_rush, penalties, turnovers, fumbles_lost, ints_thrown,
                                                possession, score ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (final_ids[i], team2, int(firstDowns[1]),thirdDowns[1],fourthDowns[1],int(totalYards[1]),int(passing[1]),completionAtt[1],float(ypp[1]),int(rushingYards[1]),int(rushingAtt[1]),float(yardsPerRushAttempt[1]),totalPenaltiesYards[1],int(turnovers[1]),int(fumblesLost[1]),int(ints[1]),possessionTime[1], score2))
                            db.commit()
                    except:
                        print sys.exc_info()[0]
                        pass
            except:
                print sys.exc_info()[0]
                pass

db.close()

