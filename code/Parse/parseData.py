# code Parse
import time
from MyMusicRecommend.code.Util.DBUtil import DBUtil
import requests
import json
import pymysql

# 连接数据库
db = pymysql.connect(host="localhost", user="root", password="ye007068", database="musicdata")

domain = "http://localhost:3000"


def getSingleList(interface="/user/record", uid=88090080, type=0):
    url = domain + interface + "?uid=" + str(uid) + "&type=" + str(type)
    r = requests.get(url)

    # 只提取歌曲和歌手和播放次数和分数和歌曲ID
    data = json.loads(r.text)
    if data['code'] == -2:
        print("无权限访问", uid)
        return
    if data['code'] == 406:
        print("操作频繁", uid)
        time.sleep(10)
        return
    data = data['allData']
    # for i in data:
    #     print(i['song']['name'], i['song']['ar'][0]['name'], i['playCount'], i['score'], i['song']['id'])
    # 存储记录 userId,songName,singer,songId,playCount,score
    sql = "INSERT INTO songinfo(userId,songName,singer,songId,playCount,score) VALUES (%s, %s, %s, %s, %s, %s)"
    for i in data:
        DBUtil().exeDML(sql,
                        uid, i['song']['name'], i['song']['ar'][0]['name'], i['song']['id'], i['playCount'], i['score'])


def parseAllUser(interface="/user/followeds", uid=9003, limit=100, offset=0):
    userList = []
    # 将所有用户的ID存入userList
    for count in range(0, 100):
        print("offset: ", offset)

        url = domain + interface + "?uid=" + str(uid) + "&limit=" + str(limit) + "&offset=" + str(offset)
        try:
            r = requests.get(url)
            time.sleep(1)
            data = json.loads(r.text)
            # print(data)

            data = data['followeds']
            for i in data:
                userList.append([i['userId'], i['nickname']])
            offset += 100
        except Exception as e:
            print(e)
            time.sleep(10)
            continue
    # print(userList)
    # 将所有用户的歌单存入数据库
    for user in userList:
        # print(user[0], user[1])
        sql = "INSERT INTO user(userId, nickname) VALUES (%s, %s)"
        DBUtil().exeDML(sql, (user[0], user[1]))


def getMusicList():
    # 获取用户ID
    userList = []
    sql = "SELECT userId FROM user"

    results = DBUtil().query_all(sql)
    for row in results:
        uid = row[0]
        # print(uid)
        userList.append(uid)
        # 获取用户歌单

    # print(userList)
    for user in userList:
        getSingleList(uid=user, type=0)  # 获取单个用户播放记录

    pass


# test() #测试
# parseAllUser()  # 爬取一部分用户的UID
# getMusicList()  # 爬取一部分用户的歌单
# 308567448
getSingleList(uid=88090080, type=0)
