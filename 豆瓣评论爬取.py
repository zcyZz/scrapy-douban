import os
import time
import requests
import re
import pandas as pd

starts = time.time()


headers = {
        'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36 Edg/102.0.1245.30'
    }

try:
    for page in range(0,3):
        time.sleep(1)
        print(f"--------------------------------------------第{page + 1}页开始爬取-------------------------------------------------")

        '''
        我们观察前后网址可知，参数limit表示一页显示的数据数量，参数start表示开始时的值，我们有两种方法实现爬取多页数据:
            ++直接修改limit属性值等于整数n，start值为0或者删去，就能获取前0-n条评论
                这里的url只需要改动limit=40即可以爬取40条数据
                https://movie.douban.com/subject/35288767/comments?limit=40&status=P&sort=new_score
                # 首页
                https://movie.douban.com/subject/35288767/comments?start=0&limit=20&status=P&sort=new_score
                # 第二页
                https://movie.douban.com/subject/35288767/comments?start=40&limit=20&status=P&sort=new_score
            ++我们完全模拟浏览器网址的改变方法，我们不改动网页每页显示的值limit，只改动start值，表示实现了翻页，然后获取多页数据,实现结果如下：
        '''

        url = f'https://movie.douban.com/subject/35288767/comments?start={page * 20}&limit=20&status=P&sort=new_score'
        html_data = requests.get(url=url,headers=headers)
        html_data.encoding = 'utf-8'
        # print(html_data.text)

        # 用户名的提取
        userName_datas = re.findall('<a href=".*?" class="">(.*)</a>', html_data.text)
        # print(len(userName_datas))

        # 评分的提取，通过观察可知评分是用户的可选择填写，因此要对它进行判别空值，确保所有列表长度一致
        score_datas = list()
        contain_score_list = re.findall('<span>看过</span>\s*(.*?)\s*<span class="comment-time', html_data.text)
        for data in contain_score_list:
            score_data = "".join(re.findall('<span class="allstar(.*) rating" title=".*?"></span>', data))
            if score_data != '':
                score_datas.append(score_data)
            else:
                score_datas.append('NAN')
        # print(len(score_datas))

        # 发表日期的提取
        time_datas = re.findall('<span class="comment-time " title="(.*)\s.*?">', html_data.text)
        # print(len(time_datas))

        # 短评正文的提取，re.S是多行匹配
        content_datas = re.findall('<span class="short">(\s?.*?\n?.*?)</span>', html_data.text, re.S)
        # print(content_datas)
        # print(len(content_datas))

        # 赞同数量的提取
        agreeNumber_datas = re.findall('<span class="votes vote-count">(.*)</span>', html_data.text)
        # print(len(agreeNumber_datas))

        df = pd.DataFrame({'用户名': userName_datas, '评分': score_datas, '发表时间': time_datas, '短评正文': content_datas, '赞同数量': agreeNumber_datas})
        # print(df)

        # 判断文件是否存在，存在就全部写入，如果存在就将表头去掉再追加
        if not os.path.exists('./豆瓣评论信息.csv'):
            df.to_csv('./豆瓣评论信息.csv', mode='a+', encoding='gb18030', index=False)
        else:
            df.to_csv('./豆瓣评论信息.csv', mode='a+', encoding='gb18030', index=False, header=False)
        print(f"--------------------------------------------第{page + 1}页爬取结束-------------------------------------------------")
        ends = time.time()
    print("全部爬取完成！")
    print('用时：', ends - starts)
except requests.exceptions.ConnectionError as e:
    print('错误：', e.args)
