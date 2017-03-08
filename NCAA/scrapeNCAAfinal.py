__author__ = 'tanyacashorali'

import sys
import jsonpickle
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

db = sqlite3.connect('/home/ec2-user/sports2016/NCAA/sports.db')

x=random.randint(1, 20)
time.sleep(x)

cur = db.execute("SELECT game_id from NCAAfinalstats")
game_ids = cur.fetchall()
game_ids = [str(g[0]) for g in game_ids]

def index():
    print "entered index"
    times = []
    final_ids = []
    today = date.today() - timedelta(days=1)
    today = today.strftime("%Y%m%d")
    f_ids = []
    vals = [50,100]
    for val in vals:
        url = urllib2.urlopen('http://scores.espn.go.com/mens-college-basketball/scoreboard/_/group/' + str(val) + '/date/' + today)
        x=random.randint(2, 4)
        time.sleep(x)
        soup = bs(url.read(), ['fast', 'lxml'])
	j = jsonpickle.decode(re.search("window\.espn\.scoreboardData\s+=\s+({.*?});", str(soup)).group(1))
        games = j['events']
        status = [g['status'] for g in games]
        statuses = [s['type']['shortDetail'] for s in status]
        index = [i for i, j in enumerate(statuses) if j == 'Final']
        ids = [game['id'] for game in games]
        final_ids = [j for k, j in enumerate(ids) if k in index]
        #final_ids = list(set(final_ids) - set(game_ids)) 
        f_ids.append(final_ids)
        
    final_ids = [item for sublist in f_ids for item in sublist]
    if(len(final_ids) == 0):
        print "No Final Box Scores yet or already recorded."
    else:
        for i in range(0, len(final_ids)):
            print final_ids[i]
            x=random.randint(3,10)
            time.sleep(x)
            espn = 'http://espn.go.com/mens-college-basketball/boxscore?gameId=' + final_ids[i]
            while True:
                try:
                    url1 = urllib2.urlopen(espn)
                except:
                    time.sleep(x)
                    continue
                break
            soup = bs(url1.read(), ['fast', 'lxml'])
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
                print "Problems getting boxscore, continuing to next game."
                continue            
            try:
                date_time = str(datetime.now())
                db.execute('''INSERT INTO NCAAfinalstats(game_id, team, fgma, tpma, ftma, oreb, dreb, reb, ast, stl, blk, turnovers, pf, pts, timestamp ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (final_ids[i], team1, tds1[2], tds1[3], tds1[4], int(tds1[5]), int(tds1[6]), int(tds1[7]), int(tds1[8]), int(tds1[9]), int(tds1[10]), int(tds1[11]), int(tds1[12]), int(tds1[13]), date_time))
                db.commit()
            except:
                print sys.exc_info()[0]
                pass
            try:
                date_time = str(datetime.now())
                db.execute('''INSERT INTO NCAAfinalstats(game_id, team, fgma, tpma, ftma, oreb, dreb, reb, ast, stl, blk, turnovers, pf, pts, timestamp ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (final_ids[i], team2, tds2[2], tds2[3], tds2[4], int(tds2[5]), int(tds2[6]), int(tds2[7]), int(tds2[8]), int(tds2[9]), int(tds2[10]), int(tds2[11]), int(tds2[12]), int(tds2[13]), date_time))
                db.commit()
            except:
                print sys.exc_info()[0]
                pass

## used for historical scraping
#dtstart = datetime(2014,12,18)
#hundredDaysLater = dtstart + timedelta(days=55)

#for dt in rrule.rrule(rrule.DAILY, dtstart=dtstart, until=hundredDaysLater):
#    print dt.strftime("%Y%m%d")
#    index(dt.strftime("%Y%m%d"))

index()
db.close()


