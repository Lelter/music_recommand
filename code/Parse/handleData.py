# 将数据清洗
import pymysql
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# plt中文字体设置
matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 用黑体显示中文
matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号

db = pymysql.connect(host="localhost", user="root", password="ye007068", database="musicdata")


def transformData(data):
    data = data.astype({
        'playCount': 'int64',
        'score': 'int64'
    })
    return data


def calculateSong(data):
    # 清洗数据 查看分布
    user_playcounts = {}
    for user, group in data.groupby('userId'):
        user_playcounts[user] = group['playCount'].sum()
    # sns.distplot(list(user_playcounts.values()), bins=5000, kde=False)
    # plt.xlim(0, 10000)
    # plt.xlabel('play_count')
    # plt.ylabel('nums of user')
    # plt.show()
    temp_user = [user for user in user_playcounts.keys() if user_playcounts[user] > 500]
    temp_playcounts = [playcounts for user, playcounts in user_playcounts.items() if playcounts > 500]
    # print('歌曲播放量大于2000的用户数量占总体用户数量的比例为',
    #       str(round(len(temp_user) / len(user_playcounts), 4) * 100) + '%')
    # print('歌曲播放量大于2000的用户产生的播放总量占总体播放总量的比例为',
    #       str(round(sum(temp_playcounts) / sum(user_playcounts.values()) * 100, 4)) + '%')
    # print('歌曲播放量大于2000的用户产生的数据占总体数据的比例为',
    #       str(round(len(data[data.userId.isin(temp_user)]) / len(data) * 100, 4)) + "%")
    data = data[data.userId.isin(temp_user)]
    song_playcounts = {}
    for song, group in data.groupby('songId'):
        song_playcounts[song] = group['playCount'].sum()
    # sns.distplot(list(song_playcounts.values()), bins=5000, kde=False)
    # plt.xlim(0, 200)
    # plt.xlabel('play_count')
    # plt.ylabel('nums of song')
    # plt.show()
    temp_song = [song for song in song_playcounts.keys() if song_playcounts[song] > 50]
    temp_playcounts = [playcounts for song, playcounts in song_playcounts.items() if playcounts > 50]

    # print('播放量大于50的歌曲数量占总体歌曲数量的比例为',
    #       str(round(len(temp_song) / len(song_playcounts), 4) * 100) + '%')
    # print('播放量大于50的歌曲产生的播放总量占总体播放总量的比例为',
    #       str(round(sum(temp_playcounts) / sum(song_playcounts.values()) * 100, 4)) + '%')
    # print('播放量大于50的歌曲产生的数据占总体数据的比例为',
    #       str(round(len(data[data.songId.isin(temp_song)]) / len(data) * 100, 4)) + "%")
    data = data[data.songId.isin(temp_song)]
    return data


def calculateSinger(data):
    # 查看歌手的分布
    singer_playcounts = {}
    for singer, group in data.groupby('singer'):
        singer_playcounts[singer] = group['playCount'].sum()
    plt.figure(figsize=(12, 8))
    # 删除歌手播放量小于200的歌手
    temp_single = [singer for singer in singer_playcounts.keys() if singer_playcounts[singer] > 200]
    data = data[data.singer.isin(temp_single)]
    # print(temp_single)  # 歌手播放量

    return data

    # 词云
    wc = WordCloud(width=1000, height=800, font_path='wordcloud/fonts/STFangSong.ttf', background_color='white', )
    wc.generate_from_frequencies(singer_playcounts)
    plt.imshow(wc)
    plt.axis('off')
    plt.show()


def recommendBasedPopularity(data):
    # 基于流行度的推荐
    tempDf = data
    songPeoplePlay = {}
    for song, group in tempDf.groupby('songName'):
        songPeoplePlay[song] = group['playCount'].sum()
    songPeoplePlay = sorted(songPeoplePlay.items(), key=lambda x: x[1], reverse=True)
    print(songPeoplePlay[:10])  # top10


def calculateScore(data):
    # 删除评分小于10的歌曲
    data = data[data.score > 10]
    return data
    pass


def userItemScoreSave(data):
    user_averageScore = {}
    for user, group in data.groupby('userId'):
        user_averageScore[user] = group['playCount'].mean()  # 计算每个用户的平均点击量
    data['score'] = data.apply(lambda x: x['playCount'] / user_averageScore[x['userId']], axis=1)
    userItemScore = data[['userId', 'songId', 'score']]
    userItemScore.rename(columns={'userId': 'user', 'songId': 'item', 'score': 'rating'}, inplace=True)
    print(userItemScore.head(10))
    # userItemScore保存到数据库
    cursor = db.cursor()
    # 先清空表
    cursor.execute("truncate table user_item_score")
    sql = "insert into user_item_score(user,item,rating) values(%s,%s,%s)"
    for index, row in userItemScore.iterrows():
        try:
            cursor.execute(sql, (row['user'], row['item'], row['rating']))
        except Exception as e:
            print(e)
    db.commit()
    cursor.close()
    # 阅读器
    # reader = Reader(line_format='user item rating', sep=',')
    # # 载入数据
    # raw_data = Dataset.load_from_df(userItemScore, reader=reader)
    # # 分割数据集
    # kf = KFold(n_splits=5)
    # # 构建模型
    # knn_itemcf = KNNBasic(k=40, sim_options={'user_based': False})
    # # 训练数据集，并返回rmse误差
    # for trainset, testset in kf.split(raw_data):
    #     knn_itemcf.fit(trainset)
    #     predictions = knn_itemcf.test(testset)
    #     accuracy.rmse(predictions, verbose=True)

    # userSongs = {}
    # # 用户听过的歌曲集合
    # for user, group in userItemScore.groupby('user'):
    #     userSongs[user] = group['item'].unique().tolist()
    # # 歌曲集合
    # songs = userItemScore['item'].unique().tolist()
    # 歌曲id和歌曲名称对应关系
    songID_titles = {}
    for index in data.index:
        songID_titles[data.loc[index, 'songId']] = data.loc[index, 'songName']
    print(songID_titles)
    # 保存到数据库
    cursor = db.cursor()
    for songId, songName in songID_titles.items():
        # 如果有重复,覆盖插入
        sql = "insert into songid_songname(songId,songName) values(%s,%s) on duplicate key update songName=%s"
        cursor.execute(sql, (songId, songName, songName))
    db.commit()

    pass


def main():
    cursor = db.cursor()
    sql = "SELECT * FROM songinfo"  # 列名为userId,songName,singer,songId,playCount,score
    data = pd.read_sql(sql, db)
    data = transformData(data)  # 格式转换
    print(data.info())
    data = calculateSong(data)  # 清洗数据
    data = calculateSinger(data)  # 查看歌手分布
    # data = calculateScore(data)  # 查看评分分布
    recommendBasedPopularity(data)
    # data.to_csv('data.csv', index=False)  # 查看数据
    userItemScoreSave(data)


# 读取数据

if __name__ == '__main__':
    main()
