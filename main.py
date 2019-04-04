# encoding:utf-8
import json
import os
import time
from urllib import request
import re
import loguru
from bs4 import BeautifulSoup

logger = loguru.logger
folder = './data'
headers = {
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,la;q=0.7,pl;q=0.6",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
}

platforms = ['国资控股', '国资参股', '上市控股', '上市参股', '风投系', '银行系', '民营系']


def down_html(url, retry=3):
    try:
        req = request.Request(url=url, headers=headers)
        resp = request.urlopen(req, timeout=5)

        if resp.status != 200:
            logger.error('url open error. url = {}'.format(url))
        html_doc = resp.read()
        if html_doc is None or html_doc.strip() == '':
            logger.error('NULL html_doc')
        return html_doc
    except Exception as e:
        logger.error("failed and retry to download url {}".format(url))
        if retry > 0:
            time.sleep(1)
            retry -= 1
            return download_html(url)


def main():
    base_url = "https://www.wdzj.com/dangan/search?filter&currentPage="

    for p in range(1, 2):
        url = base_url+str(p)
        logger.info("page = %s" % (str(p)))
        html = down_html(url)
        soup = BeautifulSoup(html, 'lxml')
        terraceList = soup.find('ul', {'class': 'terraceList'})
        items = terraceList.findAll('li', {'class': 'item'})
        for item in items:
            entity = {}
            itemTitle = item.find('div', {'class': 'itemTitle'})
            entity['title'] = itemTitle.h2.text.strip().split('\n')[0]
            entity['tags'] = [t.text.strip() for t in itemTitle.findAll(
                'div', {'class': 'itemTitleTag tag'})]

            clearfix = item.find('div', {'class': 'itemCon clearfix'})
            itemConLeft = clearfix.find('a', {'class': 'itemConLeft'})
            entity['boxs'] = [box.text.strip() for box in itemConLeft.findAll(
                'div', {'class': 'itemConBox'})]
            fname = entity['title']+'.json'
            json.dump(entity, open(os.path.join(folder, fname), 'w'))
            logger.info("%s dump!" % (str(entity['title'])))


def post_process():
    f1 = open('neo4j_format_entity.txt', 'w')
    f2 = open('neo4j_format_relation.txt', 'w')

    for fname in os.listdir(folder):
        organ = {}
        item = json.load(open(os.path.join(folder, fname), 'r'))
        title = item['title'].strip()
        platform = ''
        address = item['boxs'][2].strip()
        address = address[len('注册地：'):].split('|')
        for p in platforms:
            if ''.join(item['tags']).find(p) != -1:
                platform = p
                break
        if platform == '':
            logger.info('remove % s as it has no platform.' % (title))
            os.remove(os.path.join(folder, fname))

        organ['名称'] = item['title'].split('\n')[0]
        for tag in item['tags']:
            try:
                k, v = tag.split('\n')[0].strip(), tag.split('\n')[2].strip()
                organ[k] = v
            except IndexError:
                continue
        organ['参考利率'] = item['boxs'][0].split('\n')[1].strip()
        organ['待还余额'] = item['boxs'][1].split('：')[1].strip()
        organ['上线时间'] = item['boxs'][3].split('：')[1].strip()
        organ['网友印想及评分'] = re.sub(r'\s', '', item['boxs'][-1]).strip()
        organ['注册地'] = address[0]
        # title platform address
        entity_organ = "(%s:机构  %s),\n" % (organ['名称'], str(organ))
        entity_platform = "(%s:平台  %s),\n" % (platform, {'名称': platform})
        entity_address = "(%s:地址  %s),\n" % (
            organ['注册地'], {'名称': organ['注册地']})
        rel_organ_platform = '(%s)-[:属于]->(%s),\n' % (organ['名称'], platform)
        rel_organ_address = '(%s)-[:注册]->(%s),\n' % (organ['名称'], organ['注册地'])
        f1.write(entity_organ)
        f1.write(entity_platform)
        f1.write(entity_address)
        f2.write(rel_organ_platform)
        f2.write(rel_organ_address)

    f1.close()
    f2.close()


# 实体：平台，机构，注册地
if __name__ == '__main__':
    main()
    post_process()
