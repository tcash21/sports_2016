__author__ = 'tanyacashorali'

import jsonpickle
import random
import urllib2
import time
import re
import random
from datetime import date
from dateutil import tz
import datetime
import os
import sqlite3
import pandas as pd
from urlparse import urlparse
from bs4 import BeautifulSoup as bs

from_zone = tz.gettz('UTC')
to_zone = tz.gettz('America/New_York')

db = sqlite3.connect('/home/ec2-user/sports2016/NBA/sports.db')

x=random.randint(1, 20)
time.sleep(x)


def index():
    print "entered index"
    times = []
    halftime_ids = []
    today = date.today()
    today = today.strftime("%Y%m%d")
    url = urllib2.urlopen('http://scores.espn.go.com/nba/scoreboard?date=' + today)
    soup = bs(url.read(), ['fast', 'lxml'])
    data=re.search('window.espn.scoreboardData.*{(.*)};</script>', str(soup)).group(0)
    jsondata=re.search('({.*});window', data).group(1)
    j=jsonpickle.decode(jsondata)
    games=j['events']
    status = [game['status'] for game in games]
    half = [s['type']['shortDetail'] for s in status]
    index = [i for i, j in enumerate(half) if j == 'Halftime']
    ids = [game['id'] for game in games]
    halftime_ids = [j for k, j in enumerate(ids) if k in index]

    league = 'nba'
    if(len(halftime_ids) == 0):
        print "No Halftime Box Scores yet."
    else:
        for i in range(0, len(halftime_ids)):
            x=random.randint(5, 10)
            time.sleep(x)
            espn = 'http://espn.go.com/nba/game?gameId=' + halftime_ids[i]
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
                      
            x=random.randint(1,3) 
            time.sleep(x)
            espn = 'http://scores.espn.go.com/' + league + '/boxscore?gameId=' + halftime_ids[i]
            url = urllib2.urlopen(espn)
            soup = bs(url.read(), ['fast', 'lxml'])
            boxscore = soup.findAll('table', {'class':'mod-data'})
            highlight1 = boxscore[0].findAll('tr', {'class':'highlight'})
            highlight2 = boxscore[1].findAll('tr', {'class':'highlight'}) 
            team1_tds=highlight1[0].findAll('td')
            team2_tds=highlight2[0].findAll('td')           
            tds1 = [td.text for td in team1_tds]
            tds2 = [td.text for td in team2_tds]
            team1 = soup.findAll('span', {'class':'abbrev'})[0].text
            team2 = soup.findAll('span', {'class':'abbrev'})[1].text
            #try:
                #with db:
                #    db.execute('''INSERT INTO NBAgames(game_id, team1, team2, game_date, game_time) VALUES(?,?,?,?,?)''', (halftime_ids[i], team1, team2, gdate, game_time))
                #    db.commit()
            try:
                date_time = str(datetime.datetime.now())
                db.execute('''INSERT INTO NBAStats(game_id, team, fgma, tpma, ftma, oreb, dreb, reb, ast, stl, blk, turnovers, pf, pts, timestamp ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (halftime_ids[i], team1, tds1[2], tds1[3], tds1[4], int(tds1[5]), int(tds1[6]), int(tds1[7]), int(tds1[8]), int(tds1[9]), int(tds1[10]), int(tds1[11]), int(tds1[12]), int(tds1[14]), date_time))
                db.commit()
            except sqlite3.IntegrityError:
                print sqlite3.Error
            try:
                date_time = str(datetime.datetime.now())
                db.execute('''INSERT INTO NBAStats(game_id, team, fgma, tpma, ftma, oreb, dreb, reb, ast, stl, blk, turnovers, pf, pts, timestamp ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (halftime_ids[i], team2, tds2[2], tds2[3], tds2[4], int(tds2[5]), int(tds2[6]), int(tds2[7]), int(tds2[8]), int(tds2[9]), int(tds2[10]), int(tds2[11]), int(tds2[12]), int(tds2[14]), date_time))
                db.commit()
            except sqlite3.IntegrityError:
                print sqlite3.Error            
            # except sqlite3.IntegrityError:
            #        print 'Record Already Exists'    
            #except:
            #    print 'No Boxscore Available'     
index()
db.close()

