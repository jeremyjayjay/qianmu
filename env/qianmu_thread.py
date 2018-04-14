'''
# 03 函数封装 + 多线程版
'''

import threading
from queue import Queue
import requests
import lxml.etree
import time


# 初始URL
START_URL = 'http://qianmu.iguye.com/ranking/1528.htm'
# 生成队列对象link_queue
link_queue = Queue()
# 初始化线程池列表,设为空
threads = []
# 设置线程池大小,预设为20,不宜过大,过大可能引起异常
DOWNLOADER_NUM = 10
# 初始化总爬取页数,设为0
download_pages = 0

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
    global link_queue
    selector = lxml.etree.HTML(html)
    links = selector.xpath('//div[@class="rankItem"]//td//a/@href')
    for link in links:
        if not link.startswith('http://'):
            link = 'http://qianmu.iguye.com/%s' % link
        link_queue.put(link)

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

# 循环使用线程池里的URL下载和解析学校详情信息
def downloader():
    while True:
        link = link_queue.get()
        # 当link=None,退出程序
        if link is None:
            break
        parse_university(fetch(link))
        link_queue.task_done()
        # 显示剩余的线程
        print('remaining queue:%s'%link_queue.qsize())

if __name__ == '__main__':
    start_time = time.time()
    # 爬取得到起始页所有的学校URL列表
    links = parse(fetch(START_URL,raise_err=True))
    # 使用线程池执行
    for i in range(DOWNLOADER_NUM):
        # 生成线程对象t
        t = threading.Thread(target=downloader)
        # 启动线程
        t.start()
        # 将线程添加进线程池
        threads.append(t)

    # 当线程池为空时,往线程池添加None,作为退出程序信号
    for i in range(DOWNLOADER_NUM):
        link_queue.put(None)

    # 结束当前线程
    for t in threads:
        t.join()

    # 计算耗时
    cost_time = time.time() - start_time
    print('download %s pages,cost %s seconds'%(download_pages,cost_time))
