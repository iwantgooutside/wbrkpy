from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
import sqlite3
import ssl

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect("myspider.sqlite")
cur = conn.cursor()
cur.executescript('''
CREATE TABLE IF NOT EXISTS Pages(id INTEGER PRIMARY KEY,url TEXT UNIQUE,html TEXT,error INTEGER,old_rank REAL,new_rank REAL);
CREATE TABLE IF NOT EXISTS Links(from_id INTEGER,to_id INTEGER);
CREATE TABLE IF NOT EXISTS Webs(url TEXT UNIQUE);
''')

cur.execute("SELECT id,url FROM Pages WHERE html is NULL AND error IS NULL ORDER BY RANDOM() LIMIT 1")
row = cur.fetchone()
if row is not None :
    print("Restarting existing crawl. if you want a new crawl please delete myspider.sqlite file")
else:
    starturl = input("if you want quit then enter quit.  please enter url where you want to crawl: ")
    if starturl == "quit" :
        quit()
    elif len(starturl) < 1 : starturl = "http://www.dr-chuck.com/"
    elif starturl.endswith("/") : starturl = starturl[:-1]
    elif starturl.endswith(".htm") or starturl.endswith(".html") :
        pos = starturl.rfind("/")
        starturl = starturl[:pos]
    web = starturl
    if len(web) > 1 :
        cur.execute("INSERT OR IGNORE INTO Webs(url) VALUES(?)",(web,))
        cur.execute("INSERT OR IGNORE INTO Pages(url,html,new_rank) VALUES(?,NULL,1.0)",(web,))
        conn.commit()

cur.execute("SELECT url FROM webs")
webs = list()
for row in cur :
    webs.append(str(row[0]))
print("now we retrieve url from this webs: ",webs)

many = -1
while True :
    if many < 1 :
        try:
            print("if you want to quit then please enter 0 .")
            many = int(input("How many url do you want crawl : "))
        except:
            print("please enter a number.")
            continue
        if many == 0 :
            break
    many = many - 1

    cur.execute("SELECT id,url FROM Pages WHERE html IS NULL AND error IS NULL ORDER BY RANDOM() LIMIT 1")
    try:
        row = cur.fetchone()
        from_id = row[0]
        url = row[1]
    except:
        print("there is no unretrieved pages found. i am going to quit this program.")
        break

    print("i am trying crawl this pages: ",from_id,url,end=" ")
    data = urlopen(url,context=ctx)
    try:
        data = urlopen(url,context=ctx)
        html = data.read()
        if data.getcode() != 200 :
            print("error on pages : ",data.getcode())
            cur.execute("UPDATE Pages SET error = ? WHERE url = ?",(data.getcode(),url))
        if data.info().get_content_type() != "text/html" :
            print("ignore no html page")
            cur.execute("DELETE FROM Pages WHERE url = ?",(url,))
            commit()
            continue
        print("(" + str(len(html)) + ")",end=" ")
        soup = BeautifulSoup(html,"html.parser")
    except KeyboardInterrupt:
        print("interrupt by user")
        break
    except :
        print("failed to open or parse this url : ", url)
        cur.execute("UPDATE Pages SET error = ? WHERE url = ?",(-1,url))
        conn.commit()
        continue

    cur.execute("INSERT OR IGNORE INTO Pages(url,html,new_rank) VALUES(?,NULL,1.0)",(url,))
    cur.execute("UPDATE Pages SET html = ? WHERE url = ?",(memoryview(html),url))
    conn.commit()

    tags = soup('a')
    count = 0
    for tag in tags :
        href = tag.get("href",None)
        if href is None :
            continue

        up = urlparse(href)
        if len(up.scheme) < 1 :
            href = urljoin(url,href)

        ipos = href.find("#")
        if ipos > 1 : href = href[:ipos]
        if href.endswith(".png") or href.endswith(".jpg") or href.endswith(".gif"):continue
        if href.endswith("/"):href = href[:-1]

        if len(href) < 1:continue

        found = False
        for web in webs :
            if href.startswith(web):
                found = True
                break
        if not found : continue

        cur.execute("INSERT OR IGNORE INTO Pages(url,html,new_rank) VALUES(?,NULL,1.0)",(href,))
        count = count + 1
        conn.commit()

        cur.execute("SELECT id FROM Pages WHERE url = ? LIMIT 1",(href,))
        try:
            row = cur.fetchone()
            to_id = row[0]
        except:
            print("failed retrieve")
        cur.execute("INSERT OR IGNORE INTO Links(from_id,to_id) VALUES(?,?)",(from_id,to_id))
    print(count)
cur.close()
