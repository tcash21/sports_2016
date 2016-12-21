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

db = sqlite3.connect('/home/ec2-user/sports2016/NCAA/sports.db')

x=random.randint(1, 20)
time.sleep(x)


def index():
    print "entered index"
    times = []
    halftime_ids = []
    today = date.today()
    today = today.strftime("%Y%m%d")
    url = urllib2.urlopen('http://www.espn.com/mens-college-basketball/scoreboard/_/date/' + today)
    soup = bs(url.read(), ['fast', 'lxml'])
    data=re.search('window.espn.scoreboardData.*{(.*)};</script>', str(soup)).group(0)
    jsondata=re.search('({.*});window', data).group(1)
    j=jsonpickle.decode(jsondata)
    games=j['events']
    status = [game['status'] for game in games]
    half = [s['type']['shortDetail'] for s in status]
    index = [i for i, j in enumerate(half) if j if re.match(".*PM.*", j)]
    ids = [game['id'] for game in games]
    halftime_ids = [j for k, j in enumerate(ids) if k in index]

    league = 'nba'
    if(len(halftime_ids) == 0):
        print "No Halftime Box Scores yet."
    else:
        for i in range(0, len(halftime_ids)):
            x=random.randint(5, 10)
            time.sleep(x)
            espn = 'http://www.espn.com/mens-college-basketball/game?gameId=' + halftime_ids[i]
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
            espn = 'http://www.espn.com/mens-college-basketball/boxscore?gameId=' + halftime_ids[i]
            url = urllib2.urlopen(espn)
            soup = bs(url.read(), ['fast', 'lxml'])
            boxscore = soup.findAll('table', {'class':'mod-data'})
            team1 = soup.findAll('span', {'class':'abbrev'})[0].text
            team2 = soup.findAll('span', {'class':'abbrev'})[1].text
            try:
                with db:
                    db.execute('''INSERT INTO NCAAgames(game_id, team1, team2, game_date, game_time) VALUES(?,?,?,?,?)''', (halftime_ids[i], team1, team2, gdate, game_time))
                    db.commit()
            except:
                print 'No Boxscore Available'     
index()
db.close()

