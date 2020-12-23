import requests
import requests as rq
from bs4 import  BeautifulSoup
import  io
import time
import psycopg2
from selenium import webdriver
import Search_all_refLink_v1
import multiprocessing as mp

log = ""
raceDate = "2020/11/29"
raceCourses = ""
raceNo = ""
BasicList = []


#This function get basic info(raceDate, number of race, raceCourses) in that date
def getBasicInfo():
    table_rows=soup.find("table",{"class":"f_fs12 f_fr js_racecard"})
    total=1
    raceCourses = ""
    output = ""
    outputList = []
    for tr in table_rows.findAll('tr'):
        td=tr.findAll('td')
        img=tr.findAll('img')
        row=[i.text for i in td]

        if "沙田" in row[0]:
            raceCourses = "ST"

        if "S1" in row[0]:
            break

        for a in img:
            output = raceCourses + ":" + str(total)
            total=total+1
            outputList.append(output)

        total = 1
    return outputList

if __name__=='__main__':
    url="https://racing.hkjc.com/racing/information/chinese/Racing/Racecard.aspx?"
    DRIVER_PATH='chromedriver_win32/chromedriver'
    browser=webdriver.Chrome(executable_path=DRIVER_PATH)
    browser.get(url)
    time.sleep(2.5)
    soup=BeautifulSoup(browser.page_source,"html.parser")
    BasicList = getBasicInfo()
    print(BasicList)
    BasicList = ['ST:1']
    for i in BasicList:
        raceCourses = i.split(':')[0]
        raceNo = i.split(':')[1]
        print("Scraping "+ raceCourses + raceNo + " ...")

        url="https://racing.hkjc.com/racing/information/chinese/Racing/Racecard.aspx?RaceDate="+raceDate+"&Racecourse=" + raceCourses + "&RaceNo=" + raceNo
        #DRIVER_PATH='chromedriver_win32/chromedriver'
        #browser=webdriver.Chrome(executable_path=DRIVER_PATH)
        browser.get(url)
        time.sleep(2)

        soup=BeautifulSoup(browser.page_source,"html.parser")

        #table_rows=soup.find('span',{'class':'title_text'})
        #name_id=table_rows.getText()  # name_id = horse name and id
        #print(name_id)


        table_rows=soup.find("table",{"class":"starter f_tac f_fs13 draggable hiddenable"})
        records=[]
        hrefList = []
        count=0
        testno = 0

        for tr in table_rows.findAll('tr'):
            col=tr.findAll('td')
            row=[i.text for i in col]
            records.append(row)
            count=count+1

            for a in tr.findAll('a',href=True):
                href=a['href']
                hrefList.append(href)
                break

        #print(hrefList)


        records2=[]
        records3=[]
        rep={'\n':'','\r':''}
        for a in range(count):
            records2=[x.replace('\n','') for x in records[a]]
            # print(records2)
            records3.append(records2)

        # Connect DB
        #conn=psycopg2.connect(database="hkjc",user='postgres',password='123456',host='127.0.0.1',port='5432')
        #cursor=conn.cursor()
        # cursor.execute("DELETE FROM History") # Delete all row in table history


        TagList = []
        countTD = 0
        for i in records3[0]:
            TagList.append(i)
            countTD=countTD+1

        for cal in range(count):
            if len(records3[cal])<2:
                continue
            printlist=records3[cal]
            if cal!=0:
                try:
                    data="'"+printlist[0]+"', '"+printlist[1]+"', '"+printlist[2]+"', '"+printlist[
                        3].strip()+"', '"+printlist[4]+"', '"+printlist[5]+"', '"+printlist[6]+"', '"+printlist[
                             7]+"', '"+printlist[8]+"', '"+printlist[9]+"', '"+printlist[10].strip()+"', '"+\
                         printlist[11]+"', '"+printlist[12]+"', '"+printlist[13]+"', '"+printlist[14]+"', '"+\
                         printlist[15]+"', '"+printlist[16]+"' ,'"+printlist[17]+"'"
                    # print(data)
                    #cursor.execute("INSERT INTO History(horse_id, game_id, rank, date, game_location, distance, location_status, class, game_position, rating, trainer, jockey, dis_between_no1, winodds, pounds, running_position, finish_time, horse_weight, gear) VALUES ('"+name_id+"', "+data+")")
                    # print(name_id + ", " + data +  " Successful inserted!")
                    #conn.commit()

                    data2=""
                    for i in range(countTD):
                        data2=data2+"'"+TagList[i]+":"+printlist[i].strip()+"'"
                        if range(countTD).index(i)==countTD-1: # check is it last loop
                            break
                        else:
                            data2=data2+", "

                    log+=data2+" Successful inserted!\n"


                # cursor.execute("INSERT INTO History(game, place, date, race_place, distance, place_status, race_class, dr, rtg, trainer, jockey, lbw, winodds, act, running_position, finish_time, declar_house, gear, video) VALUES (" + data + ")")
                # cursor.execute('''INSERT INTO History(name_id) VALUES (''' + name_id + ''')''')
                # cursor.execute("INSERT INTO History(name_id) VALUES ('"+name_id+"')")
                except:
                    print("You got a error!")
                    browser.quit()
                    continue


        #---Setting a loop for get horse race history---
        #hrefList = ['/racing/information/Chinese/Horse/Horse.aspx?HorseId=HK_2020_E075']
        counttime = 0
        for link in hrefList:
            #print(link)
            url="https://racing.hkjc.com"+link+"&Option=1"
            #newhrefList.append(url)
            #Search_all_refLink_v1.SoupHistory(url)
            ap=mp.Process(target=Search_all_refLink_v1.SoupHistory,args=(url,))
            ap.start()
            counttime = counttime + 1
            # --- Set a amount for checking process in same time ---
            if counttime == 3:
                counttime = 0
                ap.join()
            # --- Set a amount for checking process in same time ---

        #---Setting a loop for get horse race history---

    #conn.commit()
    browser.quit()  # quit chrome driver


    #log = log.encode('utf-8')
    #print(log)
    file = open("RaceCardLog.txt", 'a', encoding='UTF-8')
    file.write(log)
    file.close()

    #print(log)
    #print ("It cost %f sec" % (tEnd - tStart))#會自動做近位
