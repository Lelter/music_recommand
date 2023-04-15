import joblib
import pymysql
import pandas as pd
from surprise import KNNBasic
from surprise import KNNWithMeans
from surprise import KNNWithZScore
from surprise import KNNBaseline
from surprise import SVD
from surprise import Reader, Dataset, accuracy
from surprise.model_selection import KFold
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

db=pymysql.connect(host="localhost",user="root",password="ye007068",db="musicdata",charset="utf8")
class recommendSystem:
    def __init__(self, user_item_rating, songID_titles):
        self.user_item_rating = user_item_rating
        self.songID_titles = songID_titles
        self.knn_itemcf = self.runKnnItemcf()
        self.knn_usercf = self.runKnnUsercf()
        # self.svd = self.svd()
        # self.lr = self.lr()
        # self.gbdt = self.gbdt()
        pass

    def userCF(self, userId, N=5):
        user_songs = {}
        for user, group in self.user_item_rating.groupby('user'):
            user_songs[user] = group['item'].unique().tolist()
        # 歌曲集合
        songs = self.user_item_rating['item'].unique().tolist()
        userId = str(userId)
        used_items = user_songs[userId]
        # 用户对未听过音乐的评分
        item_ratings = {}
        knn_usercf = joblib.load('Parse/knn_usercf.pkl')
        for item in songs:
            if item not in used_items:
                item_ratings[item] = knn_usercf.predict(userId, item).est
        # 找出评分靠前的5首歌曲
        song_ids = dict(sorted(item_ratings.items(), key=lambda x: x[1], reverse=True)[:N])
        song_topN = [self.songID_titles[s] for s in song_ids.keys()]
        print('推荐的歌曲：')
        for song in song_topN:
            print(song)

        return song_topN

    def itemCF(self, userId, N=5):
        user_songs = {}
        for user, group in self.user_item_rating.groupby('user'):
            user_songs[user] = group['item'].unique().tolist()
        # 歌曲集合
        songs = self.user_item_rating['item'].unique().tolist()
        userId = str(userId)
        used_items = user_songs[userId]
        print('用户听过的歌曲：')
        for item in used_items:
            print(self.songID_titles[item])
        # 用户对未听过音乐的评分
        item_ratings = {}
        knn_itemcf = joblib.load('Parse/knn_itemcf.pkl')
        for item in songs:
            if item not in used_items:
                item_ratings[item] = knn_itemcf.predict(userId, item).est

        # 找出评分靠前的5首歌曲
        song_ids = dict(sorted(item_ratings.items(), key=lambda x: x[1], reverse=True)[:N])
        song_topN = [self.songID_titles[s] for s in song_ids.keys()]
        print('推荐的歌曲：')
        for song in song_topN:
            print(song)

        return song_topN

    def runKnnItemcf(self):
        reader = Reader(line_format='user item rating', sep=',')
        # 载入数据
        raw_data = Dataset.load_from_df(self.user_item_rating, reader=reader)
        # 分割数据集
        kf = KFold(n_splits=5)
        # 构建模型
        knn_itemcf = KNNBasic(k=80, sim_options={'user_based': False, 'min_support': 3,
                                                 'name': 'pearson_baseline'})
        # 训练数据集，并返回rmse误差
        for trainset, testset in kf.split(raw_data):
            knn_itemcf.fit(trainset)
            predictions = knn_itemcf.test(testset)
            accuracy.rmse(predictions, verbose=True)
        # 保存模型
        joblib.dump(knn_itemcf, 'Parse/knn_itemcf.pkl')

        return knn_itemcf
        pass

    def runKnnUsercf(self):
        reader = Reader(line_format='user item rating', sep=',')
        # 载入数据
        raw_data = Dataset.load_from_df(self.user_item_rating, reader=reader)
        # 分割数据集
        kf = KFold(n_splits=5)
        # 构建模型
        knn_usercf = KNNBasic(k=40, sim_options={'user_based': True})
        # 训练数据集，并返回rmse误差
        for trainset, testset in kf.split(raw_data):
            knn_usercf.fit(trainset)
            predictions = knn_usercf.test(testset)
            accuracy.rmse(predictions, verbose=True)
        joblib.dump(knn_usercf, 'Parse/knn_usercf.pkl')

        return knn_usercf
        pass


def main():
    # 读取数据库

    sql = "SELECT * FROM user_item_score"  # 列名为user,item,rating
    user_item_rating = pd.read_sql(sql, db)
    # 读取歌曲id和歌曲名称对应关系
    sql = "SELECT * FROM songid_songname"  # 列名为songId,songName
    songID_titles = pd.read_sql(sql, db)
    # 转化为字典
    songID_titles = dict(zip(songID_titles.songId, songID_titles.songName))
    recommend = recommendSystem(user_item_rating, songID_titles)
    recommend.itemCF(2099359193, 15)
    # recommend.userCF(308567448, 15)


if __name__ == '__main__':
    main()