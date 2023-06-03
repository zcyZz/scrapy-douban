import os
import time
import re
import pandas as pd
import asyncio
import aiohttp
import logging



starts = time.time()   # 程序开始时间
# 请求头
headers = {
        'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36 Edg/102.0.1245.30'
    }

# 以日志的形式输出文件内容（可用系统自带的print输出）
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')  # 格式化输出

# 网页url的普通格式，以satart作为变量传入start参数，实现翻页
index_url = 'https://movie.douban.com/subject/35288767/comments?start={start}&limit=20&status=P&sort=new_score'
page_size = 20   # 有网页结构可知，网页第一页包含的评论数量为20
page_number = 20   # 爬取网页数量为10
concurrency = 5  # 并发量为5
semaphore = asyncio.Semaphore(concurrency)    # 控制并发量，避免服务器崩溃
session = None   # 创建session对象

# 爬取表页，通用爬取方法
async def get(url):
    ## try-except语句是为了避免程序出错，而导致下面的操作无法继续执行
    try:
        logging.info('爬取网址为 %s', url)   # 打印爬取的网址

        async with session.get(url, headers=headers) as response:    # session对象想服务器发起请求
            response.encoding = 'gb18030'   # 设置编码方式为“gb18030”
            return await response.text()    # 返回response对象
    except aiohttp.ClientSession:
        logging.error('出现错误，错误网址为：%s', url, exc_info=True)    # 输出发生错误网址

# 爬取列表，由main（）函数传入page的值，进行url的拼接
async def index(page):
    url = index_url.format(start=page_size * (page-1))   # 将start准确参数传入到index_url的参数{start}中
    return await get(url)   # 将url传入get()方法中，获取网页内容

# 解析网页内容，获取我们所需要的数据信息
async def analysis(results):
    index_datas = "".join(results)   # 把长度为10的results列表转换为一个字符串
    userName_datas = re.findall('<a href=".*?" class="">(.*)</a>', index_datas)   # 匹配用户名
    # 这里与上面的不同是因为某些用户未评分，导致了初次爬取的数据这个列表的长度与其他不同，导致数据框出错
    score_datas = list()   # 构建一个列表存放评分数据。
    # 初步爬取
    contain_score_list = re.findall('<span>看过</span>\s*(.*?)\s*<span class="comment-time', index_datas)   # 匹配出一个包含着评分信息的一个大列表，长度为20
    # logging.info(contain_score_list)
    # 遍历这个列表提取评分信息
    for data in contain_score_list :
        score_data = "".join(re.findall('<span class="allstar(.*) rating" title=".*?"></span>', data))
        if score_data != '':    # 将空值赋予一个NAN
            score_datas.append(score_data)
        else:
            score_datas.append('NAN')
    time_datas = re.findall('<span class="comment-time " title="(.*)\s.*?">', index_datas)   # 匹配发表时间
    content_datas = re.findall('<span class="short">(\s?.*?\n?.*?)</span>', index_datas, re.S)   # 匹配短评内容
    agree_datas = re.findall('<span class="votes vote-count">(.*)</span>', index_datas)   # 匹配赞同数量
    # 构建数据框
    dict1 = {'用户名': userName_datas, '评分': score_datas, '发表时间': time_datas, '短评正文': content_datas, '赞同数量': agree_datas}
    df = pd.DataFrame(dict1)
    # logging.info(df)
    return save(df)   # 返回数据框df，以保存所有数据

# 将分析好的数据保存为csv文件
def save(df):
    # 判断文件是否存在，存在就全部写入，如果存在就将表头去掉再追加
    if not os.path.exists('豆瓣电影信息.csv'):
        df.to_csv('./豆瓣电影评论信息.csv', mode='a+', encoding='gb18030', index=False)
    else:
        df.to_csv('./豆瓣电影评论信息.csv', mode='a+', encoding='gb18030', index=False, header=False)

# 主函数
async def main():
    global session   # 声明全局变量
    session = aiohttp.ClientSession()   # 创建session对象
    # 通过循环，获取全部url任务task，通过ensure_future来定义task对象
    index_tasks = [asyncio.ensure_future(index(page)) for page in range(1, page_number+1)]   # 创建一个事件循环对象，通过循环将我们需要爬取的url通过index()函数进行拼接，得到全部要爬取的url任务列表
    results = await asyncio.gather(*index_tasks)   # gather方法运行
    logging.info('结果是：%s', results)
    await session.close()
    return await analysis(results)   # 将results传入analysis方法中继续执行，以供analysis方法分析信息



if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())   # 执行主函数
    end = time.time()   # 程序结束时间
    print('cost time:', end-starts)   # 打印耗时


