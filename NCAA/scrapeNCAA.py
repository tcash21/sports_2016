__author__ = 'tanyacashorali'

import jsonpickle
import random
import urllib2
import time
import re
import random
from datetime import date, timedelta
from dateutil import tz
import datetime
import os
import sqlite3
import pandas as pd
from urlparse import urlparse
from bs4 import BeautifulSoup as bs

db = sqlite3.connect('/home/ec2-user/sports2016/NCAA/sports.db')

from_zone = tz.gettz('UTC')
to_zone = tz.gettz('America/New_York')

x=random.randint(4, 10)
time.sleep(x)

cur = db.execute("SELECT game_id from NCAAstats")
game_ids = cur.fetchall()
game_ids = [str(g[0]) for g in game_ids]

def index():
    print "entered index"
    times = []
    halftime_ids = []
    today = date.today()
    today = today.strftime("%Y%m%d")
    vals = [50,100]
    h_ids = []
    for val in vals:
        url = urllib2.urlopen('http://scores.espn.com/mens-college-basketball/scoreboard/_/group/' + str(val) + '/date/' + today)
        soup = bs(url.read(), ['fast', 'lxml'])
        j = jsonpickle.decode(re.search("window\.espn\.scoreboardData\s+=\s+({.*?});", str(soup)).group(1))
	games = j['events']
	status = [g['status'] for g in games]
        statuses = [s['type']['shortDetail'] for s in status]	
        index = [i for i, j in enumerate(statuses) if j == 'Halftime']
        ids = [game['id'] for game in games]
        halftime_ids = [j for k, j in enumerate(ids) if k in index]
        halftime_ids = list(set(halftime_ids) - set(game_ids))
	print(halftime_ids)
        if len(halftime_ids) > 0:
            h_ids.append(halftime_ids)     
        else:
            continue
    if len(h_ids) > 0:
        halftime_ids = [item for sublist in h_ids for item in sublist]
    league = 'ncaa'
    if len(halftime_ids) == 0:
        print "No Halftime Box Scores yet."
    else:
        for i in range(0, len(halftime_ids)):
            x=random.randint(2, 5)
            time.sleep(x)
            espn = 'http://espn.go.com/mens-college-basketball/game?gameId=' + halftime_ids[i]
            url = urllib2.urlopen(espn)
            soup = bs(url.read(), ['fast', 'lxml'])	
            game_date = soup.findAll('div', {'class':'game-date-time'})[0]
            the_date = re.search('(\\d{4}\\-\\d{2}\\-\\d{2})', str(game_date)).group(1)
            game_time = re.search('T(.*?)Z', str(game_date)).group(1)            
            the_utc = the_date + ' ' + game_time
            t=datetime.datetime.strptime(the_utc, "%Y-%m-%d %H:%M")
            t = t.replace(tzinfo=from_zone)
            est = t.astimezone(to_zone)
            gdate = est.strftime('%m/%d/%Y')
            game_time =  est.strftime('%H:%M') + ' PM ET'

            espn = 'http://scores.espn.go.com/mens-college-basketball/boxscore?gameId=' + halftime_ids[i]
            url = urllib2.urlopen(espn)
            soup = bs(url.read(), ['fast', 'lxml'])
            try:
                boxscore = soup.findAll('table', {'class':'mod-data'})
                highlight1 = boxscore[0].findAll('tr', {'class':'highlight'})
                highlight2 = boxscore[1].findAll('tr', {'class':'highlight'}) 
                team1_tds=highlight1[0].findAll('td')
                team2_tds=highlight2[0].findAll('td')           
                tds1 = [td.text for td in team1_tds]
                tds2 = [td.text for td in team2_tds]
                team1 = soup.findAll('span', {'class':'abbrev'})[0].text
                team2 = soup.findAll('span', {'class':'abbrev'})[1].text

                if team1 == 'WM':
                    team1 = 'W&M'
                if team2 == 'WM':
                    team2 = 'W&M'
                if team1 == 'TAM':
                    team1 = 'TA&M'
                if team2 == 'TAM':
                    team2 = 'TA&M'
            except:
                print "Problem with boxscore, continuing to next game."
                continue
            try:
                date_time = str(datetime.datetime.now())
                db.execute('''INSERT INTO NCAAstats(game_id, team, fgma, tpma, ftma, oreb, dreb, reb, ast, stl, blk, turnovers, pf, pts, timestamp ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (halftime_ids[i], team1, tds1[2], tds1[3], tds1[4], int(tds1[5]), int(tds1[6]), int(tds1[7]), int(tds1[8]), int(tds1[9]), int(tds1[10]), int(tds1[11]), int(tds1[12]), int(tds1[13]), date_time))
                db.commit()
            except sqlite3.IntegrityError:
                print sqlite3.Error
            try:
                date_time = str(datetime.datetime.now())
                db.execute('''INSERT INTO NCAAstats(game_id, team, fgma, tpma, ftma, oreb, dreb, reb, ast, stl, blk, turnovers, pf, pts, timestamp ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (halftime_ids[i], team2, tds2[2], tds2[3], tds2[4], int(tds2[5]), int(tds2[6]), int(tds2[7]), int(tds2[8]), int(tds2[9]), int(tds2[10]), int(tds2[11]), int(tds2[12]), int(tds2[13]), date_time))
                db.commit()
            except sqlite3.IntegrityError:
                print sqlite3.Error            
index()
db.close()


