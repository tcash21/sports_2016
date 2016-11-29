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

db = sqlite3.connect('/home/ec2-user/sports2016/NCF/sports.db')

x=random.randint(3,10)
time.sleep(x)

#url = urllib2.urlopen('https://www.sportsbook.ag/sbk/sportsbook4/nba-betting/nba-2nd-half-lines.sbk')
url = urllib2.urlopen('https://www.sportsbook.ag/sbk/sportsbook4/ncaa-football-betting/2nd-half-lines.sbk')
soup = bs(url.read(), ['fast', 'lxml'])
divs=soup.findAll('div', {'class':'col-sm-12 eventbox'})
awayTeams=[d.findAll('span', {'id':'firstTeamName'}) for d in divs]
homeTeams=[d.findAll('span', {'id':'secondTeamName'}) for d in divs]
awayTeams=[a[0].text for a in awayTeams]
homeTeams=[h[0].text for h in homeTeams]
awayTeams=filter(len, awayTeams)
homeTeams=filter(len, homeTeams)
awayTeams=[re.sub(' \(N\)', '', a) for a in awayTeams]
homeTeams=[re.sub(' \(N\)', '', h) for h in homeTeams]

market=[d.findAll('div', {'class':'market'}) for d in divs]
market=filter(len, market)
the_lines=[m[0].text for m in market]
the_spreads=[m[1].text for m in market]

lines = []
spreads = []

for i in range(0, len(the_lines)):
    try:
        if the_lines[i] == '-':
            lines.append('-')
        if the_spreads[i] == '-':
            spreads.append('-')
        lines.append(re.search('(\d+\\.?\d+)\\(', the_lines[i]).group(1))
        spreads.append(re.search('([+-]?\d+\\.?\d?)\\(', the_spreads[i]).group(1))
    except:
        next


#today = date.today()
#today = today.strftime("%m/%d/%Y")
today = str(datetime.datetime.now() - timedelta(hours=2))[0:10]
today = time.strftime("%m/%d/%Y", time.strptime(today, '%Y-%m-%d'))

date_time = str(datetime.datetime.now())

for i in range(0, len(lines)):
    try:
        with db:
            db.execute('''INSERT INTO NCFSBHalfLines(away_team, home_team, line, spread, game_date, game_time) VALUES(?,?,?,?,?,?)''', (awayTeams[i], homeTeams[i], lines[i], spreads[i], today, date_time))
            db.commit()
    except sqlite3.IntegrityError:
        print 'Record Exists'

for i in range(0, len(lines)):
    try:
        with db:
            db.execute('''INSERT INTO NCFSBteamlookup(sb_team, espn_abbr) VALUES (?,?)''', (awayTeams[i], None))
            db.execute('''INSERT INTO NCFSBteamlookup(sb_team, espn_abbr) VALUES (?,?)''', (homeTeams[i], None))
    except:
        print 'Record Exists'

db.close()
