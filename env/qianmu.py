'''
# 01 基础版:直接爬取,不使用函数封装,不使用多线程,不使用分布式,不使用框架
'''

import time
import requests
import lxml.etree


# 起始URL
START_URL = 'http://qianmu.iguye.com/ranking/1528.htm'

# 使用request请求获取起始页的响应
r = requests.get(START_URL)

# 得到起始页响应的文本信息r.text
selector = lxml.etree.HTML(r.text)

# 使用xpath匹配得到学校详情页的URL列表
links = selector.xpath('//div[@class="rankItem"]//td//a/@href')

# 初始化爬取的总页数
download_pages = 0

# 记录起始时间点
start_time = time.time()

# 循环爬取url对应的学校信息
for link in links:
    # 判断爬取的URL是否符合语法要求
    if not link.startswith('http://'):
        # 不符合语法要求的加上正确的前缀补全URL
        link = 'http://qianmu.iguye.com/%s' % link
    print(link)

    # 得到学校详情页的响应
    r = requests.get(link)

    # 得到学校信息详情页响应的文本信息r.text,并去掉/t字符
    selector = lxml.etree.HTML(r.text.replace('\t',''))

    # 去掉selector中无学校详情页URL的元素
    try:
        table = selector.xpath('//*[@id="wikiContent"]/div[1]/table/tbody')[0]
    except:
        pass

    # 爬取总页数自加1
    download_pages += 1

    # 学校详情页列表左侧信息kyes
    keys = table.xpath('./tr/td[1]//text()')

    # 学校详情页列表右侧信息values
    cols = table.xpath('./tr/td[2]')

    values = [''.join(col.xpath('.//text()')) for col in cols] # 此处的处理可以解决由于行数左右不对齐而得不到正确的keys和values对应的问题

    # 组合得到正确的学校信息info
    info = dict(zip(keys,values))
    print(info)

# 计算总耗时
cost_time = time.time() - start_time

# 打印爬取个数及总耗时
print('download %s pages,cost %s seconds'%(download_pages,cost_time))



# 本人测试结果:总爬取411个学校信息,耗时223s
