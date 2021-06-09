#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/3/29 23:32
# @Author  : yer
# @email    : caoenye@gmail.com
# @File    : platform_spider.py

# import scrapy
import warnings
warnings.filterwarnings(action='ignore', category=UserWarning, module='gensim')
import time, random
from tqdm import tqdm
import os
import requests
import json
from multiprocessing import Process, Pool
from random import randint

from log_analysis import Log_analysis
# from db_ralated import Db_ralated
import sys
sys.path.append("..")
from tools import Tools

RANDOM_TIMES = 3

class Qimai_spider(object):
    log = Log_analysis()
    # db = Db_ralated()
    tools = Tools()
    date = "2020-04-14"
    device = "iphone"
    country = "cn"
    sdate = "2014-01-01 00:00:00"
    edate = "2018-12-12 23:59:59"
    rank_sdate = '2014-01-01 00:00:00'

    def get_params_url(self, brand, page_type, genre="0", page="1", appid=0, sdate="", edate=""):
        """
        获取各种类型页面的url请求参数

        :param page_type: 该页面的类型，不同类型页面有不同的参数设置
        :param fee_type: free, paid, grossing
        :param genre:
        :param page: for comments
        :return: params:除analysis之外的参数，
        """
        # Step 1, Init params

        params = {
            "country": self.country,
            "appid": appid
        }
        url = ""
        if page_type == "index":
            params = {
                "brand": brand,
                "country": self.country,
                "genre": genre,
                "device": self.device,
                "date": self.date,
                "page": page,
                "is_rank_index": "1"
            }
            url = "/rank/index"
        elif page_type == "appinfo":
            url = "/app/appinfo"
        elif page_type == "baseinfo":
            url = "/app/baseinfo"
        elif page_type == "version":
            url = "/app/version"
        elif page_type == "comment":  # 这个的参数应该有变化，时间期限
            params = {
                "country": self.country,
                "appid": appid,
                "sword": "",
                "sdate": sdate,
                "edate": edate,
                'commentType': 'undelete',
                "page": page
            }
            url = "/app/comment"
        elif page_type == "samePubApp":  # 不知道有没有page参数
            url = "/app/samePubApp"
        elif page_type == "rank":
            params = {
                "appid": appid,
                "country": self.country,
                "brand": brand,
                "day":"1",
                "appRankShow":"1",
                "subclass": "all",
                "simple":"1",
                "rankType": "day",
                "sdate": sdate,
                "edate": edate,
                "rankEcharType":"1"
            }
            url = "/app/rankMore"
        elif page_type == 'comment_rate':
            params = {
                'appid': appid,
                'country': self.country
            }
            url = '/app/commentRate'

        analysis = self.log.get_analysis(params, url)
        params["analysis"] = analysis
        return params, url

    def get_comment_page(self, brand, page_type, id, proxy, sdate="", edate=""):
        """
        获取评论的页数
        :param brand:
        :param page_type:
        :param id:
        :param proxy:
        :param sdate:
        :param edate:
        :return:
        """
        params, url = self.get_params_url(brand, page_type, appid=str(id),
                                          sdate=sdate, edate=edate)
        contents = self.log.http_get(params, url, proxy)
        if not contents:
            print('fail to get maxPages')
            raise Exception('fail to get maxPages')  # 这里抛出异常的话，整个app的数据就不会被写入，在下一次爬的时候会被发现。
        try:
            max_page = contents["maxPage"]
            print(' %s max pages: %s' % (id, max_page))
        except Exception as e:
            print("maxPage get fall %s %s %s" % (id, e, contents))
        else:
            return max_page

    def st1_get_ids(self, genre, max_page=20):
        """
        抓取brand-genre-ids, genre-name
        :return:
        """
        brands = ["free", "paid"]
        max_page = 20
        page_type = "index"
        app_ids ={}

        for brand in brands:
            app_ids[brand] = []
            time.sleep(random.random() * RANDOM_TIMES)
            for page in range(1, max_page + 1):
                params, url = self.get_params_url(brand, page_type, genre=genre,
                                                  page=str(page))
                contents = self.log.http_get(params, url)
                # print(contents)
                try:
                    for item in contents["rankInfo"]:
                        app_ids[brand].append(item["appInfo"]["appId"])
                except Exception as e:
                    print(e, contents)
                    break
        print("%s finished!" % genre)

        return app_ids

    def st2_get_version(self, id, brand):
        """
        获取APP的版本内容
        :param id:
        :param brand:
        :return:
        """
        page_type = "version"

        # 控制爬取的速度
        # time.sleep(random.random() * RANDOM_TIMES)
        params, url = self.get_params_url(brand, page_type, appid=id)
        self.proxy = self.log.rasdial()
        contents = self.log.http_get(params, url, self.proxy)
        name = contents['version'][0]['app_name']
        return contents['version'], name

    def st3_get_comment(self, id, brand):
        """
        利用多进程来爬取所有的评论页面内容
        :return:
        """
        page_type = "comment"
        comments = []
        failed_pages = {}
        failed_pages[id] = []  # 第一个值是总的页数，第二个值是失败的页数，最后可以根据失败率来重新爬取app

        proxy = self.log.rasdial()
        max_page = self.get_comment_page(brand, page_type, id, proxy, sdate=self.sdate, edate=self.edate)
        failed_pages[id].append(max_page)
        print("%s id: %s, max_page:%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), id, max_page))
        pool = Pool(processes=10)
        results = []
        filed_page = 0
        # for page in tqdm(range(1, max_page + 1)):  # max_page + 1
        #     results.append(pool.apply_async(self.comment_pages, args=(id, page, brand, page_type)))  # 非阻塞的执行进程

        # 有的APP评论页面太多，显示爬取的进度条
        pbar = tqdm(total=max_page)

        def update(*a):
            pbar.update()
            # tqdm.write(str(a))

        for page in range(1, pbar.total+1):
            results.append(pool.apply_async(self.comment_pages, args=(id, page, brand, page_type), callback=update))

        pool.close()
        pool.join()

        for res in results:
            if res:
                try:
                    comments.extend(res.get())
                except Exception:
                    print(res.get())
            else:
                filed_page += 1
        print("%s filed pages:%s" % (id, filed_page))
        return comments

    def comment_pages(self, id, page, brand, page_type):
        """
        具体的评论内容获取
        :param id:
        :param page:
        :param brand:
        :param page_type:
        :return:
        """
        # print(page)
        time.sleep(random.random() * RANDOM_TIMES)
        proxy = self.log.rasdial()
        params, url = self.get_params_url(brand, page_type, appid=id, page=str(page),
                                          sdate=self.sdate, edate=self.edate)
        contents = self.log.http_get(params, url, proxy)
        if contents:
            return contents["appComments"]
        else:
            return []

    def st4_get_rank(self, id, brand):
        """
        获取一段时间内的app排名情况
        :param id:
        :return:
        """
        page_type = "rank"

        id_rank = {}
        params, url = self.get_params_url(brand, page_type, appid=id, sdate=self.rank_sdate, edate=self.edate)
        proxy = self.log.rasdial()
        print(url)
        contents = self.log.http_get(params, url, proxy)
        try:  # 其他错误 {'msg': '未获取到排行数据', 'code': 20000}
            id_rank[id] = contents["data"]
        except Exception as e:
            print(e, 'app 排行数据未获取到')
        return id_rank

    def st3_1_sta_comment(self):
        """
        评论的统计信息，针对每个genren，每个brand类型（free，paid）
        统计评论页面数量，并画出分布图，看不同的类别之间有没有什么大的区别
        后面可以看：排名和评论量的关系
        那就统计，{id：评论量, },后面根据需要看取出sta.keys()，或者是按照id将数据进行排序
        :return: {‘free’：{id:num, id:num, ...}, 'paid':{}}
        """
        root = "../../AppData"
        brands = ["free", "paid"]
        info = self.tools.read_json("%s/genre_name.json" % root)
        genres, g_name = info["genre"], info['name']

        for g, n in zip(genres, g_name):
            com_count = {}
            path = "%s/%s_%s" % (root, g, n)
            s_p = "%s/com_count.json" % path
            p_ids = "%s/ids.json" % path

            ids = self.tools.read_json(p_ids)
            for brand in brands:
                com_count[brand] = {}
                for id in ids:
                    r_p = "%s/%s_comments/%s.json" % (path, brand, id)
                    comments = self.tools.read_json(r_p)
                    com_count[brand][id] = len(comments)

            self.tools.write_json(s_p, com_count)

    def st2_1_sta_version(self):
        """
        版本的统计信息，针对每个genren，每个brand类型
        统计版本数量（时间对齐？），并给出分布图，看不同的类别之间有没有什么区别
        :return:
        """
        pass

    def st5_get_comment_rate(self, id, brand):
        page_type = 'comment_rate'
        params, url = self.get_params_url(brand, page_type, appid=id)
        proxy = self.log.rasdial()
        contents = self.log.http_get(params, url, proxy)
        return contents['rateInfo']

    def platform_main(self):
        """
        对id进行循环，并分成多个进程
        爬取指定APP id 的相关信息。
        :return:
        """
        # ids = self.tools.read_txt_to_list('../upper_ids.txt')
        # ids = self.tools.read_txt_to_list('../downstream_ids.txt')
        # ids = ['911699257', '590217303', '1229469444', '672662544', '869802614']
        # # names = ['滴滴车主', '千牛', '拼多多', '微店', '美团商家']
        # ids = ['911699257']
        brand = 'free'
        exist_ids = self.tools.read_dir('/home/caoenye/AppData/platform_down', match=False)
        print(exist_ids)

        for id_name in exist_ids[0]:
            id = id_name.split('_')[0]
            print(id_name)
            path = "../../AppData/platform_down/%s" % id_name
            if os.path.exists('/home/caoenye/AppData/platform_down/%s/comment.json' % id_name):  # 如果comment文件存在，就跳过该APP
                continue
            if not os.path.exists(path):
                os.makedirs(path)  # 创建文件夹
            print('%s not exists' % id_name)

            ver, name = self.st2_get_version(id, brand)
            self.tools.write_json('%s/version.json' % path, ver)

            comment_rate = self.st5_get_comment_rate(id, brand)
            self.tools.write_json('%s/comment_rate.json' % path, comment_rate)
            print('comment rate 写入完成！')

            comment = self.st3_get_comment(id, brand)
            self.tools.write_json('%s/comment.json' % path, comment)

            rank = self.st4_get_rank(id, brand)
            path = "../../AppData/platform_down/%s" % id_name
            self.tools.write_json('%s/rank1.json' % path, rank)

    def add_rank_data(self):
        """
        补充平台型APP的排名数据，页面数量较少，没有使用ip代理服务器
        :return:
        """
        brand = 'free'
        exist_ids = self.tools.read_dir('/home/caoenye/AppData/platform_down', match=False)
        print(exist_ids)
        # exist_ids = ['387682726_手机淘宝 - 淘到你说好']

        for id_name in exist_ids:
            # 由于七麦数据的限制（一次查看的数据小于五年），将14年-21年的数据分成两段来爬
            for sdate, edate in zip(['2014-01-01 00:00:00', '2018-01-02 00:00:00'], ['2018-01-01 00:00:00', '2021-01-01 00:00:00']):
                id = id_name.split('_')[0]
                path = "../../AppData/platform_down/%s" % id_name
                if os.path.exists(f'{path}/rank_{sdate.split(" ")[0]}_{edate.split(" ")[0]}.json'):
                    continue
                print(id_name)
                page_type = "rank"

                id_rank = {}
                params, url = self.get_params_url(brand, page_type, appid=id, sdate=sdate, edate=edate)
                # session = self.log.get_session()
                # print(url, params)
                response = requests.get(
                    "https://api.qimai.cn" + url,
                    headers={
                        'Referer': 'https://www.qimai.cn/rank/index/brand/free/country/cn/genre/6017/device/iphone',
                        'User-Agent': self.log.user_agent,
                    },
                    params=params,
                    cookies=self.log.get_cookies(),
                    timeout=(5, 10)
                )
                contents = json.loads(response.text)

                # contents = self.log.http_get(params, url, proxy)
                try:  # 其他错误 {'msg': '未获取到排行数据', 'code': 20000}
                    if contents['code'] == 20000:
                        print(f'{id_name}_{sdate}_{edate}未出现在排行榜中')
                        continue
                    id_rank[id] = contents["data"]
                except Exception as e:
                    print(contents)
                    print(e, 'app 排行数据未获取到')


                # print(id_rank)
                max_date = self.stamp2date(id_rank[id]['max_date'])
                min_date = self.stamp2date(id_rank[id]['min_date'])
                print(max_date, min_date)


                self.tools.write_json(f'{path}/rank_{sdate.split(" ")[0]}_{edate.split(" ")[0]}.json', id_rank)
                time.sleep(60)

    def stamp2date(self, timestamp):
        # 将时间戳数据转化成年月日数据类型
        time_local = time.localtime(timestamp / 1000)
        # 转换成新的时间格式(2016-05-05 20:28:54)
        dt = time.strftime("%Y-%m-%d", time_local)
        return dt


# 下面的内容没有用到

    def get_all_maxpages(self):
        root = "../../AppData"
        brands = ["free", "paid"]
        page_type = "comment"
        info = self.tools.read_json("%s/genre_name.json" % root)
        genres, g_name = info["genre"], info['name']  # 找到genre对应的name和id, 一共39个类别

        for g, n in zip(genres, g_name):
            if g != "6024":
                path = "%s/%s_%s" % (root, g, n)
                p_ids = "%s/ids.json" % path
                ids = self.tools.read_json(p_ids)
                for brand in brands:
                    for id in ids[brand]:
                        max_page, appCommentCount = self.get_comment_page(brand, page_type, id, sdate=self.sdate,
                                                                      edate=self.edate)

    def test_db(self):
        doc1 = {
            {"id": "819618245", "rating": 1},
            {"id": "819618242", "rating": 2}
        }
        doc2 = {
            {"id": "8196182454", "rating": 1},
            {"id": "8196182423", "rating": 2}
        }
        self.db.insert_doc("co", doc1)
        # self.db.insert_doc("comments", doc2)

    def get_appinfo(self):
        """
        appinfo只需要抓取free和paid两个类型的APP，grossing中的APP一般都包含在了这里面
        :return:
        """
        # 1.找id从数据库中，2.request，返回数据 3.解析数据，存到数据库中
        brands = ["free", "paid"]
        page_type = "appinfo"
        genres = ["6024", "7019"]
        for brand in brands:
            for genre in genres:
                # ids = self.db.find_brand_ids(brand)
                ids = self.db.find_brand_genre_ids(brand, genre)

                for id in ids:
                    appInfo = {}
                    appInfo["id"] = id
                    params, url = self.get_params_url(brand, page_type, appid=str(id))
                    contents = self.log.http_get(params, url)
                    try:
                        appInfo["appInfo"] = contents["appInfo"]
                    except Exception as e:
                        print(page_type, id, contents,
                              time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), e)
                    else:
                        self.db.insert_doc("appinfo", appInfo)

    def get_baseinfo(self):
        brands = ["free", "paid"]
        page_type = "baseinfo"
        # for brand in brands:
        #     ids = self.db.find_brand_ids(brand)
        genres = ["6024", "7019"]
        for brand in brands:
            for genre in genres:
                # ids = self.db.find_brand_ids(brand)
                ids = self.db.find_brand_genre_ids(brand, genre)

                for id in ids:
                    baseinfo = {}
                    baseinfo["id"] = id

                    params, url = self.get_params_url(brand, page_type, appid=str(id))
                    contents = self.log.http_get(params, url)
                    baseinfo["sameApp"] = []  # 有的话就加，没有就算了

                    try:
                        baseinfo["sameApp"] = contents["sameApp"]
                    except Exception as e:
                        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        print(t, page_type, id, e, "no sameApp")
                    try:
                        baseinfo["appInfo"] = contents["appInfo"]
                    except Exception as e:
                        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        print(t, page_type, id, e, "no appInfo", contents)
                    else:
                        self.db.insert_doc("baseinfo", baseinfo)


    def get_samePubApp(self):
        """
        获取同开发者应用信息
        :return:
        """
        brands = ["free", "paid"]
        page_type = "appinfo"
        for brand in brands:
            ids = self.db.find_brand_ids(brand)
            for id in ids[:10]:
                appInfo = {}
                appInfo["id"] = id
                params, url = self.get_params_url(brand, page_type, appid=str(id))
                contents = self.log.http_get(params, url)
                appInfo["appInfo"] = contents["appInfo"]
                self.db.insert_doc("appinfo", appInfo)

    def get_rank_ids(self, genre, max_page, brand='free'):
        page_type = "index"
        ids = [0] * max_page * 50

        for page in range(1, max_page + 1):
            params, url = self.get_params_url(brand, page_type, genre=genre, page=str(page))
            print(params, url)
            proxy = self.log.rasdial()
            contents = self.log.http_get(params, url, proxy)
            print(contents)
            for item in contents["rankInfo"]:
                id = item["appInfo"]["appId"]
                index = int(item["index"])
                ids[index-1] = id
        self.tools.write_json("id_list/ids_%s.json" % genre, ids)

    def get_un_spide_ids(self):
        brand, genre = "free", "6024"
        all_ids = self.db.find_brand_genre_ids(brand, genre)
        # ids = all_ids - file_ids
        file_ids = self.tools.read_dir("comment_data/row_data")
        ids = list(set(all_ids) - set(file_ids))
        self.tools.write_json("unfinished_ids.json", ids)
        print(len(all_ids), len(ids))

    def get_id_rank(self):
        """
        获取每个app的rank的变化
        :return:
        """
        genre = "6024"
        brand = 'free'
        ids = self.tools.read_json("rank_ids.json")
        page_type = "rank"
        sdate = "2015-04-01 00:00:00"
        edate = "2019-08-13 23:59:59"
        id_rank = {}
        i = 0
        for id in ids:
            params, url = self.get_params_url(brand, page_type, appid=str(id), sdate=sdate, edate=edate)
            contents = self.log.http_get(params, url)
            id_rank[id] = contents["data"]
            i = i + 1
            print(i)
        self.tools.write_json("id_rank.json", id_rank)


if __name__ == "__main__":
    qm = Qimai_spider()

    # qm.platform_main()
    qm.add_rank_data()
