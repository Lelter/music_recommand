import pandas as pd
import pymysql
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apimodel import User
import uvicorn
# 引入DBUtils
from MyMusicRecommend.code.recommend import recommendSystem
from MyMusicRecommend.code.Util.DBUtil import DBUtil
from MyMusicRecommend.code.Parse.parseData import parseData

app = FastAPI()
origins = [
    "http://localhost:3000",
    "http://localhost:8080",

]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 从数据库中获取所有用户ID
@app.get("/getAllUser")
def getAllUser():
    db = DBUtil()
    sql = "select userId from user"
    result = db.query_all(sql)
    # 整理格式
    return result


# 获取TOP50歌曲
@app.get("/TopSongs")
def TopSongs(songsNum: int = 50):
    rs = recommendSystem.TopSongs(songsNum)
    return {"status_code": 200, "playList": rs}


# 更新歌曲播放次数
@app.get("/updatePlayCount")
def updatePlayCount(userId: int, songId: int):
    db = DBUtil()
    # 如果歌曲不存在，插入歌曲
    sql = "select * from songinfo where userId = %s and songId = %s"
    result = db.query_one(sql, userId, songId)
    db = DBUtil()
    if result is None:
        net = parseData()
        # print(songId)
        result = net.netGetUserSongs(songId)
        sql = "insert into songinfo(userId,songName,singer,songId,playCount,score) values (%s,%s,%s,%s,%s,%s)"
        db.exeDML(sql, userId, result[0]["name"], result[0]["ar"][0]["name"], songId, 1, 1)
        return {"status_code": 200, "info": "插入成功"}
    # 更新播放次数
    sql = "update songinfo set playCount = playCount + 1 where userId = %s and songId = %s"
    result = db.exeDML(sql, userId, songId)
    if result == 1:
        return {"status_code": 200, "info": "更新成功"}
    else:
        return {"status_code": 400, "info": "更新失败"}


# 用户歌单详情
@app.get("/getUserSongs/{userId}")
def getUserSongs(userId: int):
    db = DBUtil()
    sql = "select songName,singer,songId,playCount from songinfo where userId = %s"
    result = db.query_all(sql, userId)
    if result is None:
        return {"playlist": "", "status_code": 404}
    songIds = []
    for song in result:
        songIds.append(song[2])
    net = parseData()
    result1 = net.netGetUserSongs(songIds)
    # 查询nickname
    db = DBUtil()
    sql = "select nickname from my_user_info where userId = %s"
    nickname = db.query_one(sql, userId)
    for i in range(len(result1)):
        result1[i]["playCount"] = int(result[i][3])
    # 整理格式
    # 去网易云拿
    playCount = 0
    for i in result:
        playCount += int(i[3])
    playlist = {
        "name": nickname[0] + "的歌单",
        "tracks": result1,
        "coverImgUrl": result1[0]["al"]["picUrl"],
        "creator": {
            "userId": userId,
            "avatarUrl": "http://p1.music.126.net/ONEXJjUnIXQn-oWWX2-GIw==/109951164797661490.jpg",
            "nickname": nickname[0],
        },
        "createTime": 1577679412747,
        "tags": [],
        "trackCount": len(result),
        "playCount": playCount,
        "description": "这是" + nickname[0] + "的歌单",
        "commentCount": 0,
        "shareCount": 0,

    }
    return {"playlist": playlist, "status_code": 200}


@app.get("/getUserRecommendSong")
def getUserRecommendSong(userId: int, type: int):
    db = pymysql.connect(host="localhost", user="root", password="ye007068", db="musicdata", charset="utf8")
    sql = "SELECT * FROM user_item_score"  # 列名为user,item,rating
    user_item_rating = pd.read_sql(sql, db)
    # 读取歌曲id和歌曲名称对应关系
    sql = "SELECT * FROM songid_songname"  # 列名为songId,songName
    songID_titles = pd.read_sql(sql, db)
    # 转化为字典
    songID_titles = dict(zip(songID_titles.songId, songID_titles.songName))
    rs = recommendSystem(user_item_rating, songID_titles)
    if str(userId) not in user_item_rating['user'].unique():
        return {"data": "", "status_code": 404}
    if type == 1:
        resultUserCF = rs.userCF(userId=userId, N=10)
        songIds = []
        for i in resultUserCF:
            songIds.append(i["id"])
        net = parseData()
        result1 = net.netGetUserSongs(songIds)
        for i in range(len(result1)):
            result1[i]["score"] = int(resultUserCF[i]["score"])
        return {
            "dailySongs": result1,
            "status_code": 200
        }
    else:
        resultItemCF = rs.itemCF(userId=userId, N=10)
        songIds = []
        for i in resultItemCF:
            songIds.append(i["id"])
        net = parseData()
        result1 = net.netGetUserSongs(songIds)
        for i in range(len(result1)):
            result1[i]["score"] = int(resultItemCF[i]["score"])
        return {
            "dailySongs": result1,
            "status_code": 200
        }


# 登陆
@app.post("/login")
def login(username: str, password: str):
    db = DBUtil()
    sql = "select * from my_user_info where username = %s and password = %s"
    result = db.query_one(sql, username, password)
    if result is None:
        return {"profile": "", "status_code": 404}
    # 仿造网易云api
    profile = {
        "userId": result[1],
        "avatarUrl": "http://p1.music.126.net/ONEXJjUnIXQn-oWWX2-GIw==/109951164797661490.jpg",
        "nickname": result[3],
    }
    return {"profile": profile, "status_code": 200}


@app.get("/user/account/{userid}")
def getUserInfo(userid: int):
    db = DBUtil()
    sql = "select * from my_user_info where userId = %s"
    result = db.query_one(sql, userid)
    if result is None:
        return {"profile": "", "status_code": 404}
    # 仿造网易云api
    profile = {
        "userId": result[1],
        "avatarUrl": "http://p1.music.126.net/ONEXJjUnIXQn-oWWX2-GIw==/109951164797661490.jpg",
        "nickname": result[3],
    }
    return {"profile": profile, "status_code": 200}


# 喜欢歌曲
@app.post("/likeMusic")
def likeMusic(userId: int, songId: int):
    db = DBUtil()
    sql = "select * from my_user_like where userId = %s and songId = %s"
    result = db.query_one(sql, userId, songId)
    db = DBUtil()
    if result is None:
        sql = "insert into my_user_like values(%s,%s)"
        result = db.exeDML(sql, userId, songId)
        if result == 1:
            return {"status_code": 200, "like": True}
        else:
            return {"status_code": 400, "like": False}
    else:
        sql = "delete from my_user_like where userId = %s and songId = %s"
        result = db.exeDML(sql, userId, songId)
        if result == 1:
            return {"status_code": 200, "like": False}
        else:
            return {"status_code": 400, "like": True}


# 获取喜欢歌曲列表
@app.get("/getLikeMusicList/{userId}")
def getLikeMusicList(userId: int):
    db = DBUtil()
    sql = "select songId from my_user_like where userId = %s"
    result = db.query_all(sql, userId)
    if result is None:
        return {"ids": [], "status_code": 404}
    songIds = []
    for i in result:
        songIds.append(i[0])
    net = parseData()
    result1 = net.netGetUserSongs(songIds)
    db = DBUtil()
    sql = "select nickname from my_user_info where userId = %s"
    nickname = db.query_one(sql, userId)
    playlist = {
        "name": nickname[0] + "的喜欢歌单",
        "tracks": result1,
        "coverImgUrl": result1[0]["al"]["picUrl"],
        "creator": {
            "userId": userId,
            "avatarUrl": "http://p1.music.126.net/ONEXJjUnIXQn-oWWX2-GIw==/109951164797661490.jpg",
            "nickname": nickname[0],
        },
        "createTime": 1577679412747,
        "tags": [],
        "trackCount": len(result),
        "description": "这是" + nickname[0] + "的喜欢歌单",
        "commentCount": 0,
        "shareCount": 0,

    }
    return {"ids": songIds, "status_code": 200, "playlist": playlist}


@app.get("/say/{data}")
def say(data: str):
    return {"message": data}


if __name__ == '__main__':
    uvicorn.run(app="main:app", host="localhost", port=8000, reload=True, debug=True)
