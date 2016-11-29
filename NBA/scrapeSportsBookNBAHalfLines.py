__author__ = 'tanyacashorali'

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
from datetime import date, timedelta

db = sqlite3.connect('/home/ec2-user/sports2016/NBA/sports.db')

x=random.randint(1, 20)
time.sleep(x)

url = urllib2.urlopen('https://www.sportsbook.ag/sbk/sportsbook4/nba-betting/2nd-half-lines.sbk')
#url = urllib2.urlopen('https://www.sportsbook.ag/sbk/sportsbook4/nba-playoffs-betting/nba-2nd-half-lines.sbk')
soup = bs(url.read(), ['fast', 'lxml'])
divs=soup.findAll('div', {'class':'col-sm-12 eventbox'})
awayTeams=[d.findAll('span', {'id':'firstTeamName'}) for d in divs]
homeTeams=[d.findAll('span', {'id':'secondTeamName'}) for d in divs]
awayTeams=[a[0].text for a in awayTeams]
homeTeams=[h[0].text for h in homeTeams]
awayTeams=filter(len, awayTeams)
homeTeams=filter(len, homeTeams)

market=[d.findAll('div', {'class':'market'}) for d in divs]
market=filter(len, market)
the_lines=[m[0].text for m in market]
the_spreads=[m[1].text for m in market]

lines = []
spreads = []

if len(the_lines) > len(the_spreads):
    upper = len(the_lines)
else:
    upper = len(the_spreads)

for i in range(0, upper):
    try:
        lines.append(re.search('(\d+\\.?\d+)\\(', the_lines[i]).group(1))
    except:
        lines.append(0)
        next

for i in range(0, upper):
    try:
        spreads.append(re.search('([+-]\d+\\.?\d?)\\(', the_spreads[i]).group(1))
    except:
	spreads.append(0)
        next

#today = str(datetime.datetime.now() - timedelta(hours=2))[0:10]
today = str(date.today())
today = time.strftime("%m/%d/%Y", time.strptime(today, '%Y-%m-%d'))

date_time = str(datetime.datetime.now())

for i in range(0, len(lines)):
    try:
        with db:
            db.execute('''INSERT INTO NBASBHalfLines(away_team, home_team, line, spread, game_date, game_time) VALUES(?,?,?,?,?,?)''', (awayTeams[i], homeTeams[i], lines[i], spreads[i], today, date_time))
            db.commit()
    except sqlite3.IntegrityError:
        print 'Record Exists'

for i in range(0, len(lines)):
    try:
        with db:
            db.execute('''INSERT INTO NBASBteamlookup(sb_team, espn_abbr) VALUES (?,?)''', (awayTeams[i], None))
            db.execute('''INSERT INTO NBASBteamlookup(sb_team, espn_abbr) VALUES (?,?)''', (homeTeams[i], None))
    except:
        print 'Record Exists'

db.close()
