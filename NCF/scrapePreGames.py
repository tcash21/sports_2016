import sys
import time
import urllib2
import re
import random
import os
import sqlite3
import jsonpickle
import pandas as pd
from urlparse import urlparse
from bs4 import BeautifulSoup as bs
from datetime import datetime
from dateutil import tz
from datetime import date, timedelta

from_zone = tz.gettz('UTC')
to_zone = tz.gettz('America/New_York')

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
divisions = ['http://espn.go.com/college-football/scoreboard/_/group/80/year/2016/seasontype/2/week/' + week_num]

for division in divisions:
    halftime_ids = []
    time.sleep(random.randint(1,3))
    url = urllib2.urlopen(division)
    soup = bs(url.read())

    data=re.search('window.espn.scoreboardData.*{(.*)};</script>', str(soup)).group(0)
    jsondata=re.search('({.*});window', data).group(1)
    j=jsonpickle.decode(jsondata)
    games=j['events']
    status = [game['status'] for game in games]
    half = [s['type']['shortDetail'] for s in status]
    index = [i for i, j in enumerate(half) if j if re.match(".*PM.*", j)]
    ids = [game['id'] for game in games]
    halftime_ids = [j for k, j in enumerate(ids) if k in index]

    if(len(halftime_ids) == 0):
        print "No Halftime Box Scores yet."
    else:
        for i in range(0, len(halftime_ids)):
            x=random.randint(2, 4)
            time.sleep(x)
            espn1 = 'http://espn.go.com/college-football/game?gameId=' + halftime_ids[i]
            url = urllib2.urlopen(espn1)
            soup = bs(url.read())
            game_date=soup.findAll("span", {"data-date": True})[0]['data-date']
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
            espn = 'http://espn.go.com/college-football/matchup?gameId=' + halftime_ids[i]
            url = urllib2.urlopen(espn)
            soup = bs(url.read())
            boxscore = soup.find('table', {'class':'mod-data'})
            team1 = soup.findAll('span', {'class':'abbrev'})[0].text
            team2 = soup.findAll('span', {'class':'abbrev'})[1].text
            try:
                with db:
                    db.execute('''INSERT INTO games(game_id, team1, team2, game_date) VALUES(?,?,?,?)''', (halftime_ids[i], team1, team2, gdate))
                    db.commit()
            except:
                print sys.exc_info()[0]
                pass

db.close()

