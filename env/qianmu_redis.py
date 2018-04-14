'''
# 04 函数封装 + 多线程 + redis分布式版
# 注:利用redis的存储原理,即可实现多线程分布式(多台电脑运行,redis存储ip均设为服务器电脑)爬取功能.
'''

import threading
import time
import signal
import requests
import lxml.etree
import redis

# 起始URL
START_URL = 'http://qianmu.iguye.com/2018USNEWS世界大学排名'
# 初始化线程池列表,设为空
threads = []
# 设置化线程池数,预设为10,可根据实际需求变更,但不建议设置过大,亲测50以内不会有问题,过大会引发异常报错
DOWNLOADER_NUM = 10
# 设置睡眠间隔时间
DOWNLOAD_DELAY = 0.1
# 初始化爬取总页数,设为0
download_pages = 0
# 生成Redis对象
r = redis.Redis()
# 初始化线程开关,设为True
thread_on = True

# 获取起始页响应文本信息的函数,对异常的URL请求时抛出异常,并过滤/t字符,返回响应文本r.text
def fetch(url,raise_err=False):
    global download_pages
    print(url)
    try:
        r = requests.get(url)
    except Exception as e:
        # 如果报错则print出来
        print(e)
    else:
        download_pages += 1
        # 如果没报错，则检查http的返回码是否正常
        if raise_err:
            r.raise_for_status()
    return r.text.replace('\t','')

# 使用xpath匹配,获取文本中所有学校详情页URL,并将其送进列表link_queue中
def parse(html):
    selector = lxml.etree.HTML(html)
    links = selector.xpath("//*[@id='content']/table/tbody/tr/td[2]/a/@href")
    for link in links:
        if not link.startswith('http://'):
            link = 'http://qianmu.iguye.com/%s' % link
        # 将url放入队列
        if r.sadd('qianmu.seen',link):
            r.lpush('qianmu.queue',link)

# 解析学校详情页信息
def parse_university(html):
    selector = lxml.etree.HTML(html)
    table = selector.xpath('//*[@id="wikiContent"]/div[1]/table/tbody')
    if not table:
        return
    table = table[0]
    keys = table.xpath('./tr/td[1]//text()')
    cols = table.xpath('./tr/td[2]')
    values = [' '.join(col.xpath('.//text()')) for col in cols]
    info = dict(zip(keys, values))
    print(info)
    # 从左侧返回队列里的qianmu元素
    r.lpush('qianmu.items',info)

# 监控thread_on信号,若为True,则触发使用线程池里的URL下载学校详情信息,
def downloader(i):
    print('Thread-%s started.'% i)
    while thread_on:
        link = r.rpop('qianmu.queue')
        if link:
            # 对link解码
            link = link.decode('utf-8')
            # 解析获取学校详情
            parse_university(fetch(link))
            # 打印当前线程结果
            print('Thread-%s %s remaining queue: %s' % (i, link, r.llen('qianmu.queue')))
        # 间隔一定时间爬取
        time.sleep(DOWNLOAD_DELAY)
    print('Thread-%s exit now.'% i)

# 设置快捷键关闭线程
def sigint_handler(signum,frame):
    print('Received Ctrl+C,wait for exit gracefully')
    global thread_on
    thread_on = False

if __name__ == '__main__':
    start_time = time.time()
    # 爬取入口页,获得学校详情页URL列表
    parse(fetch(START_URL,raise_err=True))
    # 爬取队列里所有的URL
    for i in range(DOWNLOADER_NUM):
        # 生成线程对象t
        t = threading.Thread(target=downloader,args=(i + 1,))
        # 开启线程
        t.start()
        # 将线程加入线程池列表
        threads.append(t)

    signal.signal(signal.SIGINT, sigint_handler)

    # 关闭线程
    for t in threads:
        t.join()

    # 计算耗时
    cost_time = time.time() - start_time
    print('download %s pages,cost %s seconds'%(download_pages,cost_time))
