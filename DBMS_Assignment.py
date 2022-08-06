import cx_Oracle
import csv
import random

def CreateTable():
    table1 = """
            CREATE TABLE TEAM( 
                NAME VARCHAR2(20),
                RANKING INT DEFAULT 0,
                PRIMARY KEY(NAME)
                )
            """

    table2 = """
            CREATE TABLE PLAYER(
                PLAYER_ID VARCHAR2(20),
                NAME VARCHAR2(20) NOT NULL,
                AGE INT NOT NULL,
                TOTAL_RUN INT,
                TOTAL_WICKET INT,
                BELONG_TO VARCHAR2(20),
                PRIMARY KEY(PLAYER_ID),
                FOREIGN KEY(BELONG_TO) REFERENCES TEAM(NAME) ON DELETE CASCADE
                )
            """

    table3 = """
            CREATE TABLE MATCH(
                MATCH_ID VARCHAR2(20),
                PLACE VARCHAR2(30) NOT NULL,
                DATE_OF_MATCH DATE NOT NULL,
                TEAM1 VARCHAR2(20) NOT NULL,
                TEAM2 VARCHAR2(20) NOT NULL,
                SCORE1 INT NOT NULL,
                SCORE2 INT NOT NULL,
                RESULT VARCHAR2(20) AS (CASE 
                                            WHEN SCORE1>SCORE2 THEN TEAM1 
                                            ELSE TEAM2 
                                        END),
                PRIMARY KEY(MATCH_ID),
                FOREIGN KEY(TEAM1) REFERENCES TEAM(NAME) ON DELETE CASCADE,
                FOREIGN KEY(TEAM2) REFERENCES TEAM(NAME) ON DELETE CASCADE,
                FOREIGN KEY(RESULT) REFERENCES TEAM(NAME) ON DELETE CASCADE
                )
            """

    table4 = """
            CREATE TABLE PLAYED(
                MATCH_ID VARCHAR2(20),
                PLAYER_ID VARCHAR2(20),
                RUN INT NOT NULL,
                WICKET INT NOT NULL,
                PRIMARY KEY(MATCH_ID,PLAYER_ID),
                FOREIGN KEY(MATCH_ID) REFERENCES MATCH(MATCH_ID),
                FOREIGN KEY(PLAYER_ID) REFERENCES PLAYER(PLAYER_ID)
                )
            """
    cur.execute(table1)
    cur.execute(table2)
    cur.execute(table3)
    cur.execute(table4)

id = input("Username: ")
pwd = input("Password: ")
ip = id+'/'+pwd
conn = cx_Oracle.connect(ip)
cur = conn.cursor()

try:
    CreateTable()
except:
    cur.execute("DROP TABLE PLAYED")
    cur.execute("DROP TABLE MATCH")
    cur.execute("DROP TABLE PLAYER")
    cur.execute("DROP TABLE TEAM")
    CreateTable()

timetable="TimeTable.csv"
team="Team.csv"
player="Players.csv"

with open(team,'r') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    flag=True
    for row in csvreader:
        if flag==True :
            flag=False
        else:
            query = """INSERT INTO TEAM (NAME,RANKING) VALUES(:1,:2)"""
            row[1]=int(row[1])
            cur.execute(query,row)
            
with open(player,'r') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    flag=True
    for row in csvreader:
        if flag==True :
            flag=False
        else:
            query = """INSERT INTO PLAYER (PLAYER_ID,NAME,AGE,TOTAL_RUN,TOTAL_WICKET,BELONG_TO) VALUES(:1,:2,:3,:4,:5,:6)"""
            #row[4]=int(row[4])
            #row[5]=int(row[5])
            cur.execute(query,row)
            
with open(timetable,'r') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    flag=True
    for row in csvreader:
        if flag==True :
            flag=False
        else:
            query = """INSERT INTO MATCH (MATCH_ID,PLACE,DATE_OF_MATCH,TEAM1,TEAM2,SCORE1,SCORE2) VALUES(:1,:2,:3,:4,:5,:6,:7)"""
            row.append(random.randint(130,220))
            row.append(random.randint(130,220))
            cur.execute(query,row)
            
query = """SELECT MATCH_ID,TEAM1,TEAM2,SCORE1,SCORE2 FROM MATCH"""
cur.execute(query)
result = cur.fetchall()

for x in result:
    dic1={}
    dic2={}
    match_id=x[0]
    run1=x[3]
    run2=x[4]
    wicket1=0
    wicket2=0
    
    query1 = "SELECT PLAYER_ID FROM PLAYER WHERE BELONG_TO='"+x[1]+"'"
    cur.execute(query1)
    result1 = cur.fetchall()
    
    query2 = "SELECT PLAYER_ID FROM PLAYER WHERE BELONG_TO='"+x[2]+"'"
    cur.execute(query2)
    result2 = cur.fetchall()
    
    count=0
    for i in result1:
        count+=1
        if(count==11):
            n=run1
        elif count==1 :
            n=random.randint(0,100)
        else:
            n=random.randint(0,run1)
        li1=[match_id,i[0],n,0]
        run1=run1-n
        if(run1!=0):
            wicket1+=1
        dic1[i[0]]=li1
        
        query="SELECT TOTAL_RUN FROM PLAYER WHERE PLAYER_ID='"+i[0]+"'"
        cur.execute(query)
        total_run = cur.fetchall()
        run=str((total_run[0][0])+n)
        query="UPDATE PLAYER SET TOTAL_RUN="+run+" WHERE PLAYER_ID='"+i[0]+"'"
        cur.execute(query)
        
        
    count=0
    for j in result2:
        count+=100
        if(count==11):
            n=run2
        elif count==1 :
            n=random.randint(0,100)
        else:
            n=random.randint(0,run2)
        li2=[match_id,j[0],n,0]
        run2=run2-n
        if(run2!=0):
            wicket2+=1
        dic2[j[0]]=li2
        
    if wicket1>0: wicket1-=1
    if wicket2>0: wicket2-=1
    
    for i in range(11):
        if wicket2!=0 :
            dic1[random.choice(list(dic1.keys()))][3]+=1
            wicket2-=1
    
    for j in range(11):
        if wicket1!=0 :
            dic2[random.choice(list(dic2.keys()))][3]+=1
            wicket1-=1
            
    query="""INSERT INTO PLAYED(MATCH_ID,PLAYER_ID,RUN,WICKET) VALUES(:1,:2,:3,:4)"""
    
    for i in dic1.values():
        cur.execute(query,i)
    for j in dic2.values():
        cur.execute(query,j)
        
print("\n***Everything is DONE successfully!!!***")
conn.commit()
cur.close()
conn.close()