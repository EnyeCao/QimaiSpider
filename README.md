# QimaiSpider
爬取七麦数据网站上的APP信息，包括APP排行榜、APP更新日志、APP评论、APP的一些基本信息。

## 需要爬取的量比较少时
此时不会被封ip，参照platform_spider.py中 add_rank_data函数的get方式写，直接将浏览器登录后的cookies写到 log_analysis.py 中的get_cookies方法中

## 需要爬取的量比较多时
参照platform_spider.py中 platform_main 函数的get方式写
1. 需要搭建代理服务器（更换ip）来实现持续爬。代理服务器搭建方法：https://cuiqingcai.com/4596.html； github地址： https://github.com/Germey/ADSLProxyPool
2. 需要多注册几个账号，获取session，一方面有些内容不登录是爬取不到的，另一方面防止单一账号被封。注册的账号写到log_analysis.py中的update_sessions方法中。
3. 七麦网站的加密参数analysis破解方法参考[对加密参数及压缩混淆JS的逆向分析](https://blowingdust.com/encrypted-compression-javascript-analysis.html)文章
