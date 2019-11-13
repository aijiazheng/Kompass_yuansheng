import random
from _md5 import md5
from multiprocessing import Process

import redis
import requests
import time
import urllib3
import pymongo
from lxml import etree
from redis import Redis

urllib3.disable_warnings()  # 不显示验证https的安全证书

MONGODB_HOST = '192.168.5.247'
MONGODB_PORT = 27017

MONGODB_DBNAME = 'company_info'
MONGODB_DOCNAME = 'zhilianJob_positions_10_8'
client = pymongo.MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
tdb = client[MONGODB_DBNAME]
port = tdb[MONGODB_DOCNAME]  # 表名
# 外网地址
# redis_cilent = Redis(host='171.221.241.20', port=6579, db=13,password='Gouuse@spider')
# 内网地址
redis_client = Redis(host='192.168.5.247', port=6379, db=6, password='Gouuse@spider')


def get_proxies():
    '''
    获取redis中的代理
    :return:
    '''
    num = 1
    while 1:
        try:
            redis_ip = Redis(host='192.168.5.247', db=7, password='Gouuse@spider')
            a = random.randint(0, 20)
            proxies_values = redis_ip.lindex('proxy11', a).decode()
            proxies = {
                "http": "http://" + proxies_values,
                "https": "http://" + proxies_values,
            }
            print(proxies)
            break
        except:
            # print("redis中获取代理第{}失败".format(num))
            num += 1
            # pass
    return proxies


proxies = get_proxies()

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'
}


def info(url, key, ent_id):
    while True:
        s = requests.session()
        global proxies

        try:
            response = s.get(url, headers=headers, verify=False, allow_redirects=False, timeout=40, proxies=proxies)
            break
        except Exception as e:
            # print(e)
            time.sleep(1)
            proxies = get_proxies()
    selector = etree.HTML(response.text)
    item = {}
    # 招聘详情页面地址
    job_detail_url = url
    # 公司名称
    company_name = selector.xpath(
        '//div[@class="company"]/a[1]/text()')  # //*[@id="root"]/div[4]/div[2]/div[1]/div/a[1]
    if company_name:
        if key == company_name[0]:
            # 岗位名称
            position = selector.xpath('//div[@class="summary-plane"]/div/h3/text()')
            # 招聘岗位薪资
            salary = selector.xpath('//div[@class="summary-plane"]/div/div[2]/div[1]/span/text()')
            # 招聘职位经验要求
            exp_req = selector.xpath('//div[@class="summary-plane"]/div/div[2]/div[1]/ul/li[2]/text()')
            # 岗位学历要求
            edu_req = selector.xpath('//div[@class="summary-plane"]/div/div[2]/div[1]/ul/li[3]/text()')
            # 岗位招聘人数
            needed_num = selector.xpath('//div[@class="summary-plane"]/div/div[2]/div[1]/ul/li[4]/text()')
            # 招聘职位类型
            # job_type = selector.xpath('')
            # 工作地点
            # address = selector.xpath('//*[@id="root"]/div[4]/div[1]/div[1]/div[3]/div/span/text()')
            address = selector.xpath('//*[@id="root"]//div[@class="job-address"]/div[@class="job-address__content"]/span[@class="job-address__content-text"]/text()')
            if address:
                item['working_place'] = address[0]  # 招聘地区
            else:
                item['working_place'] = ''
            # 发布时间
            # publish_time = selector.xpath('//*[@id="root"]/div[3]/div/div/div[1]/span/text()')
            publish_time = selector.xpath('//*[@id="root"]//div[@class="summary-plane__top"]/span[@class="summary-plane__time"]/text()')
            # 信息来源
            source = selector.xpath('/html/head/title/text()')

            item['job_detail_url'] = job_detail_url
            if position:
                item['job_name'] = position[0]  # 招聘职位名称
            else:
                item['job_name'] = ''
            if salary:
                item['salary'] = salary[0]
            else:
                item['salary'] = ''
            if exp_req:
                item['exp_req'] = exp_req[0]
            else:
                item['exp_req'] = ''
            if edu_req:
                item['edu_req'] = edu_req[0]
            else:
                item['edu_req'] = ''


            item['needed_num'] = needed_num[0]

            item['company_name'] = company_name[0]  # 公司名字
            item['job_release_date'] = publish_time[0]  # 职位发布时间
            item['platform_name'] = "智联招聘"  # 招聘平台名称
            item['create_time'] = int(time.time())
            item['job_type'] = ""  # 招聘职位类型
            item['ent_id'] = ent_id  # 企业id
            item['platform_id'] = ""  # 招聘平台id
            item['update_time'] = ""  # 修改时间
            item['job_expire_date'] = ""  # 职位到期时间

            print(item)
            port.insert(item)


def infoUrl(url, key, ent_id):
    while True:
        s = requests.session()
        global proxies
        try:
            res = s.get(url, verify=False, allow_redirects=False, headers=headers, timeout=40, proxies=proxies)
            break
        except Exception as e:
            # print(e)
            time.sleep(1)
            proxies = get_proxies()
    # print(res.content)
    if res.content:
        # print('json内容：{}'.format(res.json()))
        selector = res.json()
        if selector:
            code = selector['code']
            if code == 200:
                data = selector['data']['results']
                for i in data:
                    href = i['positionURL']
                    # print("职位详情页面url：{}".format(href))
                    if href:
                        info(href, key, ent_id)


def spider():
    # key='湖南万商壹站电子商务有限公司'
    # key = '四川快贷网金融服务外包有限公司合肥分公司'
    # key = '重庆蔻语营销策划有限公司'
    # key = '沈阳晟德科技有限公司'
    # key = '深圳市欧赛电子材料有限公司  '
    # key = '上海博喜鞋业有限公司'
    # key = "上海陆家嘴金融发展有限公司"
    while True:
        try:
            company_name = redis_client.lpop('a_data_for_spider_no_web_10_08_zhilian').decode('utf-8').split('|')
            # company_name = redis_client.lpop('raw_data_zhilian_500W').decode('utf-8')
            key = company_name[0]
            ent_id = company_name[1]
            # key = '四川慧盈科技有限责任公司'
            # key = '武汉蜗达网络科技有限公司'
            # ent_id = '12345'
            # key = company_name
            # ent_id = ''
            print(key)
            url = 'https://fe-api.zhaopin.com/c/i/sou?pageSize=90&cityId=489&workExperience=-1&education=-1&companyType=-1&employmentType=-1&jobWelfareTag=-1&kw={}&kt=2&at=a7a305f570914a8bb08d3cbda5a6a4c3&rt=7f61e2525d6945e79b901955fd7b5811&_v=0.74871478&userCode=720203608&x-zp-page-request-id=e76bd27c40824f0ebe052e1c7763833a-1554789649046-802867'.format(
                key)
            # infoUrl(url,key,ent_id)
            # print(url)
            try:
                if infoUrl(url, key, ent_id):
                    for i in range(2, 50):
                        num = i * 90
                        urls = 'https://fe-api.zhaopin.com/c/i/sou?start={}&pageSize=90&cityId=489&workExperience=-1&education=-1&companyType=-1&employmentType=-1&jobWelfareTag=-1&kw={}&kt=2&at=a7a305f570914a8bb08d3cbda5a6a4c3&rt=7f61e2525d6945e79b901955fd7b5811&_v=0.74871478&userCode=720203608&x-zp-page-request-id=e76bd27c40824f0ebe052e1c7763833a-1554789649046-802867'.format(
                            num, key)
                        print(urls)
                        print('下一页--------------------------------------------------------------------------', urls)
                        infoUrl(urls, key, ent_id)
            except Exception as ex:
                print(ex)
        except Exception as redis_ex:
            print(redis_ex)


import threading
from time import sleep

if __name__ == '__main__':
    for th_index in range(20):
        thread_ = threading.Thread(target=spider(), name=f'thread-{th_index:02}', daemon=True)
        thread_.start()
        sleep(1)
    while True:
        st = time.time()
        sleep(30)
        print('=================1==============================================================')
        print(f'当前活跃线程数为 {threading.activeCount()}')
        print(f'活跃线程 {threading.enumerate()}')
