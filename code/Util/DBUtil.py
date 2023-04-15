# 数据库操作封装
import pymysql


class DBUtil:
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'ye007068',
        'db': 'musicdata',
        'charset': 'utf8'}

    def __init__(self):
        self.connection = pymysql.connect(**DBUtil.config)
        self.cursor = self.connection.cursor()

    def close_file(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    # 插入 修改 删除调用
    def exeDML(self, sql, *args):
        try:
            # 执行sql
            count = self.cursor.execute(sql, args)
            # 提交事务
            self.connection.commit()
            return count
        except Exception as e:
            print(e)
            if self.connection:
                self.connection.rollback()
        finally:
            self.close_file()

    # 单句查询
    def query_one(self, sql, *args):
        try:
            # 执行sql
            self.cursor.execute(sql, args)
            # 获取结果集
            return self.cursor.fetchone()
        except Exception as e:
            print(e)
        finally:
            self.close_file()

    # 多句查询
    def query_all(self, sql, *args):
        try:
            # 执行sql
            self.cursor.execute(sql, args)
            # 获取结果集
            return self.cursor.fetchall()
        except Exception as e:
            print(e)
        finally:
            self.close_file()

    # sql = 'insert into emp (empno, ename, sal)values(%s,%s,%s)'
    # count = dbutil.exeDML(sql,9999,'xiaoxiao',12000)
    # print(count)

    # sql = 'select * from emp where empno=%s'
    # emp = dbutil.query_one(sql,7788)
    # print(emp)
