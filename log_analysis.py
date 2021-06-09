#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/12 9:31
# @Author  : yer
# @email    : caoenye@gmail.com
# @File    : log_analysis.py
import time
from scrapy.selector import Selector
import requests
requests.adapters.DEFAULT_RETRIES = 5
import re
# from urllib.parse import quote
import base64
import json
import time
import random
import tqdm
# from urllib.request import urlopen
# import urllib.parse
import joblib

from ADSLProxy.adslproxy.db import RedisClient
import sys
sys.path.append("..")
from tools import Tools

class Log_analysis(object):
    """
    该类目的为了获取analysis参数（必需的加密参数）
    获取cookie: Network-Doc 再刷新
    """

    pool_user_agent = [
        # 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/18.17763',
        # 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36'
                       ]
    user_agent = random.choice(pool_user_agent)

    def object_lh(self, a):
        e = '00000008d78d46a'
        t = len(e)
        n = len(a)
        a = list(a)
        for s in range(n):
            a[s] = chr(ord(a[s]) ^ ord(e[(s + 10) % t]))
        return ''.join(a)

    def get_analysis(self, params, url):
        """
        获取analysis参数
        :param params:
        :param url:
        :return:
        """
        o = []
        # diff_time = int(time.time()*1000 - float(cookies['synct'])*1000)
        # print(diff_time)
        t = int(time.time() * 1000 - 1515125653845)
        for value in params.values():
            o.append(str(value))
        o = ''.join(sorted(o))
        # o = quote(o)  # quote()
        o = base64.b64encode(o.encode()).decode()
        o += "@#" + url + "@#" + str(t) + "@#1"
        o = base64.b64encode(self.object_lh(o).encode()).decode()
        return o

    def get_cookies(self):
        pool_cookies = [
                    'gr_user_id=60f4c3ef-3b55-41e0-9cb3-fc6355ec8eb4; grwng_uid=0b89c94e-8a5e-4d2e-b6af-865aaf04a4a1; qm_check=SxJXQEUSChd2fHd1dRQQeV5EVVwYYhwXenQZd0ZZQFhZU0MQdlRaW1xAEHBRQlVTRAN0AQQXRENmBWsIEEBDbwVvABwVHhJaWFFbWxIKEgAcABkHHgAVABZF; PHPSESSID=ool09be7uevbu6n8iihjjs8vt5; AUTHKEY=%2Fo78qApO2iLurVwsBDnEsCAQzBNSg6MpVQZ3Q%2FVkS6vTzLtFvONxl4k1n4iCJ%2FHCqi%2FjT%2FkBvBy8PxrJHlasp5qMa7gMSZUg1aMpouPSmZ5cN6yC7fyVeg%3D%3D; ada35577182650f1_gr_last_sent_cs1=qm5695457486; aso_ucenter=7efdl4z5CqsTpiiKXqCUBS0tt7Yz%2BNCyO0KsfXaUHpcAnGaoQ8DcOLWINS9%2FnwYV; USERINFO=0Pt%2FYQXikYYN34gGSPbBehJPmMLDlEnM9NJeqmwcTxvv9X3GutuXF8Tm%2BLiXdptJj%2Fb0FIpCFU0D29YpnrgtYnI12bMQieAju5EwGs%2FS2FmZUOot6bgGBs2MFOZVsQmdgVW3lwMFH4aDCyLlMeThdQ%3D%3D; ada35577182650f1_gr_session_id=284c4008-004f-42aa-94b0-450960318d48; ada35577182650f1_gr_last_sent_sid_with_cs1=284c4008-004f-42aa-94b0-450960318d48; ada35577182650f1_gr_cs1=qm5695457486; ada35577182650f1_gr_session_id_284c4008-004f-42aa-94b0-450960318d48=true; synct=1617258996.626; syncd=-1555'
            ]
        # # 设置：过一分钟就换一个账号，获取现在是多少分钟，根据分钟数的最后一位，确定要用哪个账号爬
        # t = time.strftime("%M", time.localtime())
        # t = int(t[-1])
        # if t >= len(pool_cookies):
        #     time.sleep(20)
        #     t = random.randint(0, len(pool_cookies))
        # cookies = pool_cookies[t]

        cookies = pool_cookies[0]
        cookies = {name_value.split('=')[0]: name_value.split('=')[1]
                   for name_value in cookies.replace(' ', '').split(';')}
        cookies['synct'] = format(time.time(), '.3f')  # 将时间改为现在的时间
        return cookies

    def get_session(self):
        """
        从session池中根据时间挑一个session
        :return:
        """
        pool_sessions = joblib.load('session_pool.pkl')
        t = time.strftime("%M", time.localtime())
        t = int(t[-1])
        if t >= len(pool_sessions):
            time.sleep(20)
            t = random.randint(0, len(pool_sessions))
        session = pool_sessions[t]
        return session

    def update_sessions(self):
        """
        通过账号密码登录，并获取到session
        :return:
        """
        # 将自己的账号名和密码放在这里
        user_info = [
            {'username': 'xx', 'password': 'xx'}
        ]

        url = '/accountV1/login'
        params = {}
        analysis = self.get_analysis(params, url)
        params["analysis"] = analysis

        Session_Pool = []

        for user in user_info:
            s = requests.Session()
            print(user, params)
            response = s.post("https://api.qimai.cn" + url,
                              data=user, params=params,
                              headers={
                                  'Referer': 'https://www.qimai.cn/account/signin/r/%2F',
                                  'User-Agent': self.user_agent,
                                  'Conection': "close"
                              }
                              )
            cont = json.loads(response.text)
            print(user['username'], cont)
            if cont['code'] == 10000:
                Session_Pool.append(s)
            else:
                print('获取session出错 %s' % cont)
        joblib.dump(Session_Pool, 'session_pool.pkl')

    def http_get_cookie(self, params, url, proxy):
        flag, attempts = True, 0
        while attempts < 5:
            attempts += 1
            while not proxy:
                time.sleep(3)
                proxy = self.rasdial()
                # print('空的proxy：%s' % proxy)
            cookies = self.get_cookies()
            # print(proxy, attempts, cookies['ada35577182650f1_gr_last_sent_cs1'])  # 测试ip和尝试次数
            try:
                response = requests.get(
                    "https://api.qimai.cn" + url,
                    headers={
                        'Referer': 'https://www.qimai.cn/app/rank/appid/%s/country/cn' % params['appid'],
                        'User-Agent': self.user_agent,
                    },
                    params=params,
                    cookies=cookies,
                    proxies={"https": "http://%s" % proxy[0]},
                    timeout=(5, 10)
                )
            except Exception as e1:
                # http_get 方法失败，只能是proxy出了问题，一般没什么问题，应该是在拨号服务器在换拨号，导致之前的ip不能用
                # ，解决办法：休眠一会儿，等待换proxy
                time.sleep(3)
                proxy = self.rasdial()
                if attempts == 4:  # 如果是多次请求失败，那很有可能是代理服务器的ip不能访问qimai，那就休眠一会儿
                    print('%s 代理服务器ip %s 连不上或超时，休眠50s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), proxy))
                    time.sleep(50)
            else:
                contents = json.loads(response.text)
                if contents['code'] == 10000:
                    return contents, proxy
                elif contents['code'] == 10602:  # 这个地方失败，只会是cookies出了问题，解决办法：重试(换cookies)
                    print('%s 休眠十五分钟 %s %s %s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), contents['msg'], cookies['ada35577182650f1_gr_last_sent_cs1'], proxy))
                    #todo 如果cookies池中都被封了的话，程序就停1800秒，等待解封。 可能会用到多进程中的信息传递机制。
                    time.sleep(900)
                elif contents['code'] == 10011:
                    print('%s账号过期，结束进程， %s, %s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), contents['msg'], cookies['ada35577182650f1_gr_last_sent_cs1']))
                else:
                    print('其他错误 %s' % contents)
        return [], proxy

    def http_get(self, params, url, proxy):
        flag, attempts = True, 0
        while attempts < 5:
            attempts += 1
            while not proxy:
                time.sleep(3)
                proxy = self.rasdial()
                # print('空的proxy：%s' % proxy)
            session = self.get_session()
            # print(proxy, attempts, cookies['ada35577182650f1_gr_last_sent_cs1'])  # 测试ip和尝试次数
            try:
                response = session.get(
                    "https://api.qimai.cn" + url,
                    headers={
                        'Referer': 'https://www.qimai.cn/rank/index/brand/free/country/cn/genre/6017/device/iphone',
                        'User-Agent': self.user_agent,
                    },
                    params=params,
                    proxies={"https": "http://%s" % proxy[0]},
                    timeout=(5, 10)
                )
            except Exception as e1:
                # http_get 方法失败，只能是proxy出了问题，一般没什么问题，应该是在拨号服务器在换拨号，导致之前的ip不能用
                # ，解决办法：休眠一会儿，等待换proxy
                time.sleep(5)
                proxy = self.rasdial()
                if attempts == 4:  # 如果是多次请求失败，那很有可能是代理服务器的ip不能访问qimai，那就休眠一会儿
                    print('%s 代理服务器ip %s 连不上或超时，休眠50s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), proxy))
                    time.sleep(50)
            else:
                contents = json.loads(response.text)
                if contents['code'] == 10000:
                    return contents
                elif contents['code'] == 10602:  # 这个地方失败，只会是cookies出了问题，解决办法：重试(换cookies)
                    print('%s 账号被封，休眠十五分钟 %s %s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), contents['msg'], proxy))
                    # todo 如果cookies池中都被封了的话，程序就停1800秒，等待解封。 可能会用到多进程中的信息传递机制。
                    time.sleep(900)
                elif contents['code'] == 10011:
                    print('%s账号过期，更新session， %s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), contents['msg']))
                    # print(contents)
                    if time.strftime("%H", time.localtime()) == '02':  # 如果是晚上两点的话，等待15分钟再登录
                        print('2点了，休眠一小时')
                        time.sleep(3600)
                    time.sleep(10)
                    self.update_sessions()
                else:
                    print('其他错误 %s' % contents)
        return []

    def session(self):
        # 将自己的账号名和密码放在这里
        user_info = [
            {'username': 'xx', 'password': 'xx'}
        ]

        url = '/account/signinForm'
        params = {}
        analysis = self.get_analysis(params, url)
        params["analysis"] = analysis

        Session_Pool = []

        for user in user_info:
            s = requests.Session()
            response = s.post("https://api.qimai.cn" + url,
                                     data=user, params=params,
                        headers={
                            'Referer': 'https://www.qimai.cn/rank',
                            'User-Agent': self.user_agent,
                        },
                        )
            cont = json.loads(response.text)
            print(user['username'], cont)
            if cont['code'] == 10000:
                Session_Pool.append(s)
        joblib.dump(Session_Pool, 'session_pool.pkl')
        s_p = joblib.load('session_pool.pkl')


        sdate = "2017-09-01"
        edate = "2019-09-01"
        params = {
            "appid": '1044283059',
            "country": 'cn',
            "brand": 'free',
            "subclass": "all",
            "rankType": "day",
            "sdate": sdate,
            "edate": edate
        }
        url = "/app/rankMore"

        analysis = self.get_analysis(params, url)
        params["analysis"] = analysis
        r = s_p[1].get(
            'https://api.qimai.cn' + url, params=params,
            headers={
                'Referer': 'https://www.qimai.cn/account/signin/r/%2F',
                'User-Agent': self.pool_user_agent[0],
            }
        )
        print(json.loads(r.text))

    def get_ips(self):

        loc = 'http://http.tiqu.alicdns.com/getip3?num=30&type=2&pro=510000&city=511400&yys=0&port=11&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=2&regions='
        resp = requests.get(loc)
        res = json.loads(resp.text)
        ips = res['data']
        ips = self.judge(ips)
        print('重新获取ip len ips: %s, time: %s' % (len(ips), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        joblib.dump(ips, 'ips.pkl')

    def judge(self, ips, type='two'):
        """
        判断ip是否可以用
        :param proxy:
        :return:
        """
        Use = []
        for ip in ips:
            if type == 'two':
                proxy_dict = {"https": "http://%s:%s" % (ip["ip"], ip["port"])}
            else:
                proxy_dict = {'https': 'http://%s' % ip}
            try:
                res = requests.get('https://www.baidu.com', proxies=proxy_dict)
            except Exception as e:
                # print('%s failed' % ip)
                print(e)
                pass
            else:
                if res.status_code == 200:
                    Use.append(ip)
                    print('%s use')
        return Use

    def get_ip_list(self, pages):
        """
        从西刺ip上爬取ip地址
        :param pages:
        :return:
        """
        Proxies = []
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'
        }
        for page in range(1, pages):
            url = 'https://www.xicidaili.com/wn/' + str(page)
            res = requests.get(url, headers=headers)
            selector = Selector(text=res.text)
            results = selector.css('#ip_list tr')
            for result in results[1:]:
                ip = result.css('td::text')[0].extract()
                port = result.css('td::text')[1].extract()
                proxy = ip + ':' + port
                Proxies.append(proxy)
        return Proxies

    def rasdial(self):
        """
        从数据库中读出存储的ip地址
        :return:
        """
        client = RedisClient(host='xx', password='xx')
        proxies = client.proxies()
        # print(proxies)
        return proxies


if __name__ == '__main__':
    t = Log_analysis()
    # t.update_sessions()
    print(t.rasdial())
    # t.get_ips()
    # P = t.get_ip_list(2)
    # print(len(P))
    # Use = t.judge(P, type='one')
    # print(len(Use))
    # print(t.rasdial())
    # print(t.get_cookies()['ada35577182650f1_gr_last_sent_cs1'])
    # t.http_get2()

