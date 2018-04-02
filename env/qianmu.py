import requests
import lxml.etree

START_URL = 'http://qianmu.iguye.com/ranking/1528.htm'

r = requests.get(START_URL)

selector = lxml.etree.HTML(r.text)

links = selector.xpath('//div[@class="rankItem"]//td//a/@href')

for link in links:
    if not link.startswith('http://'):
        link = 'http://qianmu.iguye.com/%s' % link
    print(link)
    r = requests.get(link)
    selector = lxml.etree.HTML(r.text.replace('\t',''))
    try:
        table = selector.xpath('//*[@id="wikiContent"]/div[1]/table/tbody')[0]
    except:
        pass
    keys = table.xpath('./tr/td[1]//text()')
    cols = table.xpath('./tr/td[2]')
    values = [''.join(col.xpath('.//text()')) for col in cols]
    info = dict(zip(keys,values))
    print(info)