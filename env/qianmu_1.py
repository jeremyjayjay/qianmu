'''
# 02 函数封装版
'''

import time
import requests
import lxml.etree


# 起始URL
START_URL = 'http://qianmu.iguye.com/ranking/1528.htm'

# 初始化待爬取URL的列表,置为空
link_queue = []

# 初始化爬取总页数,置为0
download_pages = 0

# 获取起始页响应文本信息的函数,并过滤/t字符,返回响应文本r.text
def fetch(url):
    print(url)
    r = requests.get(url)
    return r.text.replace('\t','')

# 使用xpath匹配,获取文本中所有学校详情页URL,并将其送进列表link_queue中
def parse(html):
    global link_queue
    selector = lxml.etree.HTML(html)
    links = selector.xpath('//div[@class="rankItem"]//td//a/@href')
    link_queue += links

# 获取当前学校详情页响应文本中需要的列表信息
def parse_university(html):
    global download_pages
    selector = lxml.etree.HTML(html)
    table = selector.xpath('//*[@id="wikiContent"]/div[1]/table/tbody')

    # 如果table为空,返回继续往下获取
    if not table:
        return
    table = table[0]
    keys = table.xpath('./tr/td[1]//text()')
    cols = table.xpath('./tr/td[2]')
    values = [' '.join(col.xpath('.//text()')) for col in cols]
    info = dict(zip(keys, values))
    download_pages += 1
    print(info)

if __name__ == '__main__':

    # 开始爬取
    links = parse(fetch(START_URL))

    start_time = time.time()
    for link in link_queue:
        if not link.startswith('http://'):
            link = 'http://qianmu.iguye.com/%s' % link
        parse_university(fetch(link))

    # 计算总耗时
    cost_time = time.time() - start_time

    # 打印爬取个数及总耗时
    print('download %s pages,cost %s seconds'%(download_pages,cost_time))
