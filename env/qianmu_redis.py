import threading
import time
import signal
import requests
import lxml.etree
import redis


START_URL = 'http://qianmu.iguye.com/ranking/1528.htm'
link_queue = Queue()
threads = []
DOWNLOADER_NUM = 30
download_pages = 0
r = redis.Redis()

def fetch(url,raise_err=False):
    global download_pages
    print(url)
    try:
        r = requests.get(url)
    except Exception as e:
        print(e)
    else:
        download_pages += 1
        if raise_err:
            r.raise_for_status()
    return r.text.replace('\t','')

def parse(html):
    global link_queue
    selector = lxml.etree.HTML(html)
    links = selector.xpath('//div[@class="rankItem"]//td//a/@href')
    for link in links:
        if not link.startswith('http://'):
            link = 'http://qianmu.iguye.com/%s' % link
        link_queue.put(link)

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

def downloader():
    while thread_on:
        link = r.rpop('qianmu.queue')
        if link:
            link = link.decode
        parse_university(fetch(link))
        link_queue.task_done()
        print('remaining queue:%s'%link_queue.qsize())

def sigint_handler(signum,frame):
    print('Received Ctrl+C,wait for exit gracefully')
    global thread_on
    thread_on = False

if __name__ == '__main__':
    start_time = time.time()
    parse(fetch(START_URL,raise_err=True))
    for i in range(DOWNLOADER_NUM):
        t = threading.Thread(target=downloader,args=(i + 1,))
        t.start()
        threads.append(t)

    signal.signal(signal.SIGINT,sigint_handler)

    for t in threads:
        t.join()

    cost_time = time.time() - start_time
    print('download %s pages,cost %s seconds'%(download_pages,cost_time))