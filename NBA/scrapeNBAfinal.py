__author__ = 'tanyacashorali'

import random
import urllib2
import time
import re
import random
import datetime
import os
import sqlite3
import pandas as pd
from urlparse import urlparse
from bs4 import BeautifulSoup as bs
from dateutil import rrule
from datetime import datetime, timedelta, date

db = sqlite3.connect('/home/ec2-user/sports2016/NBA/sports.db')

x=random.randint(1, 20)
time.sleep(x)


def index():
    print "entered index"
    times = []
    today = date.today() - timedelta(days=31)
    today = today.strftime("%Y%m%d")
    url = urllib2.urlopen('http://www.espn.com/nba/scoreboard/_/date/' + today)
    soup = bs(url.read(), ['fast', 'lxml'])
    ids = re.findall('boxscore\?gameId=(\d+)', str(soup))
    
    league = 'nba'
    if(len(ids) == 0):
        print "No Final Box Scores yet."
    else:
        for i in range(0, len(ids)):
            x=random.randint(5, 10)
            time.sleep(x)
    
            espn = 'http://scores.espn.go.com/' + league + '/boxscore?gameId=' + ids[i]
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
            try:
                date_time = str(datetime.now())
                db.execute('''INSERT INTO NBAfinalstats(game_id, team, fgma, tpma, ftma, oreb, dreb, reb, ast, stl, blk, turnovers, pf, pts, timestamp ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (ids[i], team1, tds1[2], tds1[3], tds1[4], int(tds1[5]), int(tds1[6]), int(tds1[7]), int(tds1[8]), int(tds1[9]), int(tds1[10]), int(tds1[11]), int(tds1[12]), int(tds1[14]), date_time))
                db.commit()
            except sqlite3.IntegrityError:
                print sqlite3.Error
            try:
                date_time = str(datetime.now())
                db.execute('''INSERT INTO NBAfinalstats(game_id, team, fgma, tpma, ftma, oreb, dreb, reb, ast, stl, blk, turnovers, pf, pts, timestamp ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (ids[i], team2, tds2[2], tds2[3], tds2[4], int(tds2[5]), int(tds2[6]), int(tds2[7]), int(tds2[8]), int(tds2[9]), int(tds2[10]), int(tds2[11]), int(tds2[12]), int(tds2[14]), date_time))
                db.commit()
            except sqlite3.IntegrityError:
                print sqlite3.Error           

index()
db.close()


