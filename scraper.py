# import libraries
# -*- coding: utf-8 -*
import MySQLdb
import urllib2
from bs4 import BeautifulSoup
import os
import re
import sys
import thread
import time
import random
import string
import multiprocessing
reload(sys)
sys.setdefaultencoding('iso-8859-1')

# 数据库层——————————————————————————————————data base————————————————————————————————————————

# 查询topic信息


def search_db(tablename, id, name, url):
    # 打开数据库连接
    db = MySQLdb.connect(
        host='192.168.100.60',
        port=3306,
        user='qindong',
        passwd='123456',
        db='test')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    if tablename == "topic_info":
        sql = "select * from topic_info where topic_href=%s" % (
            url)
    count = cursor.execute(sql)
    db.close()
    if count != 0:
        return True
    return False


def select_topic_inDB(page):
    # 打开数据库连接
    db = MySQLdb.connect(
        host='192.168.100.60',
        port=3306,
        user='qindong',
        passwd='123456',
        db='test')
# 使用cursor()方法获取操作游标
    cursor = db.cursor()
    retrun_num = page * 100
    page_num = retrun_num - 100
    sql = "select topic_href from topic_info order by topic_id ASC limit %s,%s" % (
        page_num, retrun_num)
    cursor.execute(sql)
    results = cursor.fetchall()
    # results = cursor.fetchall()
    # 关闭数据库连接
    # for row in results:
    #     print row[0]
    db.close()
    return results

# 查询用户信息


def select_userinfo_inDB(username):
    # 打开数据库连接
    db = MySQLdb.connect(
        host='192.168.100.60',
        port=3306,
        user='qindong',
        passwd='123456',
        db='test')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    if "'" in username:
        username = username.replace("'", "")
    sql = "select user_name from user_info where user_name = '%s'" % username

    count = cursor.execute(sql)
    # results = cursor.fetchall()
    # 关闭数据库连接
    db.close()
    if count != 0:
        return True
    return False
# 创建数据表


def creattable():
    # 打开数据库连接
    db = MySQLdb.connect(
        host='192.168.100.60',
        port=3306,
        user='qindong',
        passwd='123456',
        db='test')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # 使用execute方法执行SQL语句

    # sql = "alter table message_info MODIFY message_quote VARCHAR(300)"
    # sql = """CREATE TABLE topic_info (
    #         topic_id VARCHAR(20) primary key,
    #         topic_name VARCHAR(200) NOT NULL,
    #         topic_host VARCHAR(50) NOT NULL,
    #         topic_board  VARCHAR(20) NOT NULL,
    #         topic_replies VARCHAR(50),
    #         topic_views VARCHAR(50))"""

    # sql = """CREATE TABLE user_info (
    #     id int auto_increment primary key,
    #     user_name  VARCHAR(50) NOT NULL unique,
    #     user_posts VARCHAR(20),
    #     user_gender VARCHAR(20),
    #     user_age  VARCHAR(20),
    #     user_location VARCHAR(100),
    #     user_activity VARCHAR(20),
    #     user_position VARCHAR(20),
    #     user_email VARCHAR(40),
    #     user_website VARCHAR(200),
    #     user_bitcoinaddress VARCHAR(200),
    #     user_trust VARCHAR(50),
    #     user_dateregistered VARCHAR(50))"""
    sql = """CREATE TABLE message_info (
        message_id VARCHAR(20) NOT NULL,
        message_host VARCHAR(50) NOT NULL,
        message_date  VARCHAR(50) NOT NULL,
        message_topic VARCHAR(20),
        message_quote text,
        message_reply text,
        primary key (message_id, message_date))"""
    cursor.execute(sql)
    # 使用 fetchone() 方法获取一条数据库。

    # 关闭数据库连接
    db.close()


def delete_table(tablename):
    db = MySQLdb.connect(
        host='192.168.100.60',
        port=3306,
        user='qindong',
        passwd='123456',
        db='test')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    sql = "truncate table %s" % tablename
    # sql = "ALTER TABLE BitcoinTalk ADD unique(user_name)"
    cursor.execute(sql)
    db.commit()
    db.close()


# 插入数据
# 插入主题列表
def insert_topic(topic):
    # 打开数据库连接
    db = MySQLdb.connect(
        host='192.168.100.60',
        port=3306,
        user='qindong',
        passwd='123456',
        db='test')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    sql = """insert ignore into topic_info (
        topic_id,topic_name,topic_host,
        topic_board,topic_replies,topic_views,topic_href) values (%s,%s,%s,%s,%s,%s,%s)"""
    cursor.execute(sql, (topic.topic_id, topic.topic_name, topic.topic_host,
                         topic.topic_board, topic.topic_replies, topic.topic_views, topic.topic_href))
    # 提交到数据库执行
    # 关闭数据库连接
    print "insert success"
    db.commit()
    db.close()
# 插入消息回复


def insert_message(message_list):
    # 打开数据库连接
    db = MySQLdb.connect(
        host='192.168.100.60',
        port=3306,
        user='qindong',
        passwd='123456',
        db='test')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    for message in message_list:
        # SQL语句,字段id,host,date,topic,quote
        sql = """insert ignore into message_info (
        message_id,message_host,message_date,
        message_topic,message_quote,message_reply) values (%s,%s,%s,%s,%s,%s)"""
        # 执行sql语句
        cursor.execute(sql, (message.message_id, message.message_host.encode('gbk', 'ignore'),
                             message.message_date, message.message_topic,
                             message.message_quote.encode('gbk', 'ignore'), message.message_reply.encode('gbk', 'ignore')))

    # 提交到数据库执行
    # 关闭数据库连接
    print "insert success"
    db.commit()
    db.close()


# 插入用户信息数据
def insert_user(user):
    # 打开数据库连接
    db = MySQLdb.connect(
        host='192.168.100.60',
        port=3306,
        user='qindong',
        passwd='123456',
        db='test')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL 插入语句
    sql = """insert ignore into user_info (
    user_name,user_posts,user_gender,user_age,
    user_location,user_activity,user_position,
    user_email, user_website, user_bitcoinaddress,
    user_trust,user_dateregistered)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
    # 执行sql语句
    if user.user_location is not None:
        user.user_location = user.user_location.encode(
            'gbk', 'ignore')
    if user.user_email is not None:
        user.user_email = user.user_email.encode(
            'gbk', 'ignore')
    if user.user_website is not None:
        user.user_website = user.user_website.encode('gbk', 'ignore')
    cursor.execute(sql, (user.user_name.encode('gbk', 'ignore'), user.user_posts,
                         user.user_gender, user.user_age, user.user_location,
                         user.user_activity, user.user_position, user.user_email,
                         user.user_website, user.user_bitcoinaddress, user.user_trust,
                         user.user_dateregistered))

    # 提交到数据库执行
    # 关闭数据库连接
    db.commit()
    db.close()
    print "insert success"


# 网络层—————————————————————————————————————net and soup—————————————————————————————————————
# 使用代理


def use_proxy():
    proxy = {'https': '127.0.0.1:51987'}
    proxy_support = urllib2.ProxyHandler(proxy)
    opener = urllib2.build_opener(
        proxy_support, urllib2.HTTPHandler(debuglevel=1))
    urllib2.install_opener(opener)

# 获取解析器


def get_post_soup(url):
    headers = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36"),
               "Accept": ("text/html,application/xhtml+xml,application/xml;"
                          "q=0.9,image/webp,*/*;q=0.8"),
               "Accept-Language": "zh-CN,zh;q=0.8",
               "Cookie": ("SMFCookie129=a%3A4%3A%7Bi%3A0%3Bs%3A7%3A%221110555%22%3Bi%3A"
                          "1%3Bs%3A40%3A%2283c7c796c5d9ba9bd23fe2aed455667af4150ac0%22%3Bi%3A2%3Bi"
                          "%3A1693053059%3Bi%3A3%3Bi%3A0%3B%7D; PHPSESSID=drugf9vr6tatl59"
                          "1agu8r4mbm1")
               }
    req = urllib2.Request(url, None, headers)
    page = urllib2.urlopen(req, timeout=20)
    soup = BeautifulSoup(page, 'lxml', from_encoding="iso-8859-1")
    page.close()
    return soup

# 文件管理————————————————————————————————file save and read————————————————————————————————————————

# 读文件


def read_file_as_str(file_path):
    # 判断路径文件存在
    if not os.path.isfile(file_path):
        open(file_path, 'w')
    all_the_text = open(file_path).read()
    # print type(all_the_text)
    return all_the_text
# save html as txt


def save_subject_file(file_name, contents):
    fh = open(file_name, 'w')
    fh.write(contents)
    fh.close()
    print 'save success'
# 清空文件


def clear_file(file_name):
    fh = open(file_name, 'w')
    fh.truncate()
    fh.close()
    print 'file clear success'

# 解析层———————————————————————————————————————parse—————————————————————————————————————————————

# 解析话题层~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~parse topic ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def save_topic_info():
    board_url = 'https://bitcointalk.org/index.php?board=1.0'
    soup = get_post_soup(board_url)
    page_area = soup.find('td', id="toppages")
    page_href = page_area.find_all('a', class_="navPages")
    if read_file_as_str('topic_page_href.txt') == "":
        last_page_href = board_url
    else:
        print "read the last time topiclist cache"
        last_page_href = read_file_as_str('topic_page_href.txt')
    # child Boards [5]
    # pages[6]
    # subject[8]
    # find the subject
    # there is 41 tr an 7 td,subject start from tr1 and td2,and subject name
    # from contents[3]

    last_page_count = int(last_page_href.split('=')[1].split('.')[1])
    if len(page_href):

        topic_page_count = int(page_href[-2].get('href').split('=')[1].split('.')[1])
    else:
        topic_page_count = last_page_count
    print last_page_count
    print topic_page_count
    while topic_page_count >= last_page_count:
        soup = get_post_soup(last_page_href)
        table_list = soup.find_all('table', class_="bordercolor")
        table_tr = table_list[1].find_all('tr')
        print last_page_href
        for tr in range(1, len(table_tr)):
            table_td = table_tr[tr].find_all('td')
            href = table_td[2].find('span', id=re.compile('msg')).a['href']
            id = (re.split('=', href)[1])[:-2]
            name = table_td[2].find('span', id=re.compile('msg')).a.string
            if table_td[3].find('a') is not None:
                host = table_td[3].a.string
            else:
                host = table_td[3].string.strip()
            replies = table_td[4].string.strip()
            views = table_td[5].string.strip()
            print href
            print id
            print name.encode('gbk', 'ignore')
            print host.encode('gbk', 'ignore')
            print replies
            print views
            topic = topic_info(id, name.encode('gbk', 'ignore'), host.encode(
                'gbk', 'ignore'), board_url, replies, views, href)
            insert_topic(topic)
        print "~~~~~~~~~~~~~page" + str(last_page_count) + "topicinfo~~~~~~~~~~"
        save_subject_file('topic_page_href.txt', last_page_href)
        last_page_count += 40
        last_page_href = re.sub(r'\d+$', str(last_page_count), last_page_href)


def parse_user_info_from_topic(sort_num):
    if read_file_as_str('topic_url_list_' + str(sort_num) + 'user.txt') == "complete":
        return True
    if read_file_as_str('topic_url_list_' + str(sort_num) + 'user.txt') == "":
        topic_url_tuple = list(select_topic_inDB(sort_num))
        topic_url_list = []
        for topic_url in topic_url_tuple:
            topic_url_list.append(str(topic_url[0]))
    else:
        print "read the last time url_list cache "
        topic_url_list = re.split(
            r'--', read_file_as_str('topic_url_list_' + str(sort_num) + 'user.txt'))
    topic_list_sample = list(topic_url_list)
    strhref = "--".join(topic_list_sample)
    save_subject_file('topic_url_list_' + str(sort_num) + 'user.txt', strhref)
    for url in re.split(
            r'--', read_file_as_str('topic_url_list_' + str(sort_num) + 'user.txt')):
        if url != "https://bitcointalk.org/index.php?topic=2059111.0":
            parse_onetopic_userinfo(url, sort_num, topic_list_sample)
    strcom = "complete"
    save_subject_file('topic_url_list_' + str(sort_num) + 'user.txt', strcom)
    return True


def parse_message_info_from_topic(sort_num):
    if read_file_as_str('topic_url_list_' + str(sort_num) + 'message.txt') == "complete":
        return True
    if read_file_as_str('topic_url_list_' + str(sort_num) + 'message.txt') == "":
        topic_url_tuple = list(select_topic_inDB(sort_num))
        topic_url_list = []
        for topic_url in topic_url_tuple:
            topic_url_list.append(str(topic_url[0]))
    else:
        print "read the last time message_list cache "
        topic_url_list = re.split(
            r'--', read_file_as_str('topic_url_list_' + str(sort_num) + 'message.txt'))
    topic_list_sample = list(topic_url_list)
    strhref = "--".join(topic_list_sample)
    save_subject_file('topic_url_list_' + str(sort_num) + 'message.txt', strhref)
    for url in re.split(
            r'--', read_file_as_str('topic_url_list_' + str(sort_num) + 'message.txt')):
        if url != "https://bitcointalk.org/index.php?topic=2059111.0":
            parse_onetopic_messageinfo(url, sort_num, topic_list_sample)
    strcom = "complete"
    save_subject_file('topic_url_list_' + str(sort_num) + 'message.txt', strcom)

# 爬取一个主题中信息回复


def parse_onetopic_messageinfo(url, sort_num, topic_list_sample):
    print url
    soup = get_post_soup(url)
    page_area = soup.find('td', class_="middletext")
    page_href = page_area.find_all('a', class_="navPages")
    if read_file_as_str('message_page_href' + str(sort_num) + '.txt') == "":
        last_page_href = url
    else:
        print "read the last time pagelist cache"
        last_page_href = read_file_as_str('message_page_href' + str(sort_num) + '.txt')
        if re.sub(r'\d+$', '0', last_page_href) != url:
            clear_file('user_page_href' + str(sort_num) + '.txt')
            parse_onetopic_userinfo(url, sort_num, topic_list_sample)
    last_page_count = int(last_page_href.split('=')[1].split('.')[1])
    if len(page_href):
        if str(page_href[-1].string).isdigit():
            topic_page_count = int(page_href[-1].get('href').split('=')[1].split('.')[1])
        elif str(page_href[-2].string).isdigit():
            topic_page_count = int(page_href[-2].get('href').split('=')[1].split('.')[1])
    else:
        topic_page_count = last_page_count
    print last_page_count
    print topic_page_count
    while topic_page_count >= last_page_count:
        soup = get_post_soup(last_page_href)
        # time.sleep(random.randint(0, 2))
        insert_message(get_onepage_message_info(last_page_href))
        print "~~~~~~~~~~~~~page" + str(last_page_count) + "messageinfo~~~~~~~~~~"
        save_subject_file('message_page_href' + str(sort_num) + '.txt', last_page_href)
        last_page_count += 20
        last_page_href = re.sub(r'\d+$', str(last_page_count), last_page_href)
    print "the topic message_info parse complete !"
    clear_file('message_page_href' + str(sort_num) + '.txt')
    topic_list_sample.remove(url)
    strhref = "--".join(topic_list_sample)
    save_subject_file('topic_url_list_' + str(sort_num) + 'message.txt', strhref)
    return True
# 爬取一个主题用户数据


def parse_onetopic_userinfo(url, sort_num, topic_list_sample):
    print url
    soup = get_post_soup(url)
    page_area = soup.find('td', class_="middletext")
    page_href = page_area.find_all('a', class_="navPages")
    if read_file_as_str('user_page_href' + str(sort_num) + '.txt') == "":
        last_page_href = url
    else:
        print "read the last time pagelist cache"
        last_page_href = read_file_as_str('user_page_href' + str(sort_num) + '.txt')
        if re.sub(r'\d+$', '0', last_page_href) != url:
            clear_file('user_page_href' + str(sort_num) + '.txt')
            parse_onetopic_userinfo(url, sort_num, topic_list_sample)

    last_page_count = int(last_page_href.split('=')[1].split('.')[1])
    if len(page_href):
        if str(page_href[-1].string).isdigit():
            topic_page_count = int(page_href[-1].get('href').split('=')[1].split('.')[1])
        elif str(page_href[-2].string).isdigit():
            topic_page_count = int(page_href[-2].get('href').split('=')[1].split('.')[1])
    else:
        topic_page_count = last_page_count
    print last_page_count
    print topic_page_count
    while topic_page_count >= last_page_count:
        soup = get_post_soup(last_page_href)
        if read_file_as_str('user_url_list' + str(sort_num) + '.txt') == "":
            user_list_sample = get_onepage_poster_url(last_page_href)
        else:
            print "read the last time user_list cache "
            user_list_sample = list(
                re.split(r'--', read_file_as_str('user_url_list' + str(sort_num) + '.txt')))
        if len(user_list_sample):
            parse_user_form(user_list_sample, sort_num)
        print "~~~~~~~~~~~~~page" + str(last_page_count) + "userinfo~~~~~~~~~~"
        save_subject_file('user_page_href' + str(sort_num) + '.txt', last_page_href)
        last_page_count += 20
        last_page_href = re.sub(r'\d+$', str(last_page_count), last_page_href)
    clear_file('user_page_href' + str(sort_num) + '.txt')
    print "the topic user_info parse complete !"
    topic_list_sample.remove(url)
    strhref = "--".join(topic_list_sample)
    save_subject_file('topic_url_list_' + str(sort_num) + 'user.txt', strhref)
    return True

# 解析每一页~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~parse page~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 获取回复消息列表

# 递归查找消息内容


def find_text(content):
    contents = ""
    if str(type(content)) == "<class 'bs4.element.NavigableString'>":
        str_replies = content.string
        contents += str_replies + " "
        return contents
    elif str(type(content)) == "<class 'bs4.element.Tag'>":
        for item in content:
            find_text(item)
    return contents

# 获取一页消息回复


def get_onepage_message_info(url):
    print url
    soup = get_post_soup(url)
    form = soup.find('form', id="quickModForm")
    message_list = form.find('table', class_="bordercolor")
    main_message = message_list.tr['class']
    user_message = message_list.find_all('tr', class_=main_message)
    message_info_list = []
    count = 0
    for message in user_message:
        if message.find('td', class_="poster_info").a is not None:
            message_host = message.find('td', class_="poster_info").a.string
        else:
            message_host = message.find('td', class_="poster_info").b.string
        subject = message.find('div', class_="subject")
        post = message.find('div', class_="post")
        message_quote = []
        message_reply = []
        # 去除空引用
        if post.find('div', class_="quoteheader") is not None:
            i = 0
            while i < len(post.contents):
                if i < 0:
                    break
                if post.contents[i] != "" and str(post.contents[i].name) == "div":
                    if post.contents[i].attrs.has_key('class'):
                        if str(post.contents[i].get('class')[0]) == "quoteheader" and post.contents[i].find('a') is None:
                            post.contents.pop(i)
                            post.contents.pop(i)
                            i -= 2
                        elif post.contents[i] != "" and str(post.contents[i].name) == "div" and str(post.contents[i].get('class')[0]) == "quoteheader"and post.contents[i].find('a') is not None and "bitcointalk" not in post.contents[i].find('a').get('href'):
                            post.contents.pop(i)
                            post.contents.pop(i)
                            i -= 2
                i += 1
        if post.find('div', class_="quoteheader") in post.contents:
            while True:
                index_list = []
                # 寻找quoteheader 位置
                for index in range(0, len(post.contents)):
                    if str(type(post.contents[index])) == "<class 'bs4.element.Tag'>":
                        if post.contents[index] in post.find_all('div', class_="quoteheader"):
                            index_list.append(index)
                print index_list
                # 如果引用在前
                if index_list[0] == 0:
                    for index in range(0, len(index_list)):
                        href = post.contents[index_list[index]].a.get('href')
                        s1 = re.search(r'msg\d+', href).group()
                        quote_id = re.search(r'\d+', s1).group()
                        if index + 1 < len(index_list):
                            contents = ""
                            for item in post.contents[index_list[index] + 2:index_list[index + 1]]:
                                if str((type(item))) == "<class 'bs4.element.NavigableString'>":
                                    str_replies = item.string
                                    contents += str_replies + " "
                            message_quote.append(str(index + 1) + "||" + quote_id +
                                                 "||" + contents)
                        # 最后一个引用提取
                        elif index + 1 == len(index_list):
                            if post.contents[index_list[index] + 2:] is not None:
                                contents = ""
                                for item in post.contents[index_list[index] + 2:]:
                                    if str((type(item))) == "<class 'bs4.element.NavigableString'>":
                                        str_replies = item.string
                                        contents += str_replies + " "
                                message_quote.append(str(index + 1) + "||" + quote_id +
                                                     "||" + contents)
                    break
                else:
                    # 如果内容在前引用在后
                    contents = ""
                    for content in post.contents[0:index_list[0]]:
                        if str((type(content))) == "<class 'bs4.element.NavigableString'>":
                            str_replies = content.string
                            contents += str_replies + " "
                    message_reply.append(contents)
                    post.contents = post.contents[index_list[0]:]
        # 如果没有引用
        elif post.find('div', class_="quoteheader") not in post.contents:
            contents = ""
            for content in post.contents:
                contents += find_text(content)
            #     if str(type(content)) == "<class 'bs4.element.Tag'>":
            #         for subcontent in content.contents:
            #             if str((type(subcontent))) == "<class 'bs4.element.NavigableString'>":
            #                 str_replies = subcontent.string
            #                 contents += str_replies + " "
            #     if str(type(content)) == "<class 'bs4.element.NavigableString'>":
            #         str_replies = content.string
            #         contents += str_replies + " "
            # contents.append(content.encode("utf-8", 'ignore'))
            message_reply.append(contents)
        message_href = subject.a.get('href')
        s1 = re.split(r'=', message_href)
        message_topic = re.split(r'\.', s1[1])[0]
        s2 = re.split(r'\.', s1[1])[1]
        message_id = re.findall(re.compile('\d+'), s2)[0]
        message_date = subject.next_sibling.next_sibling.string
        str_quote = "--".join(message_quote)
        str_reply = "--".join(message_reply)
        mes = message_info(message_id, message_host,
                           message_date, message_topic, str_quote, str_reply)
        message_info_list.append(mes)
        count += 1
        print message_id
        print message_host.encode('gbk', 'ignore')
        print message_date
        print message_topic
        print message_quote
        print message_reply
        print '~~~~~~~~~~~~~~~~~~~~~~~~' + str(count) + '~~~~~~~~~~~~~~~~~~~~~~~~~'

    return message_info_list
# 获取一页用户信息地址


def get_onepage_poster_url(url):
    soup = get_post_soup(url)
    form = soup.find('form', id="quickModForm")
    message_list = form.find('table', class_="bordercolor")
    main_message = message_list.tr['class']
    user_message = message_list.find_all('tr', class_=main_message)
    user_url_list = []
    for user in user_message:
        poster = user.find('td', class_="poster_info")
        if poster.b.a is not None:
            if not (select_userinfo_inDB(poster.b.a.string.encode('gbk', 'ignore'))):
                user_url = poster.find('a').get('href')
                user_url_list.append(user_url)
    # 用户信息板块
    # user_list = user_message.find_all('td', class_="poster_info")
    # # 回复内容板块
    # reply_message = user_message.find_all('td', class_="td_headerandpost")
    # 删除无用信息
    user_set = set(user_url_list)
    user_list = list(user_set)
    print len(user_set)
    print user_set
    return user_list
# 解析具体表单~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~parse form~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 解析用户信息表格


def parse_user_form(url_set, sort_num):
    user_list_sample = url_set
    user_url_list = list(user_list_sample)
    if len(user_list_sample):
        count = 0
        for url in user_list_sample:
            time.sleep(random.randint(0, 2))
            print url
            soup = get_post_soup(url)
            table_list = soup.select('table')
            # 寻找表单
            # self, name, posts,gender,age,location, activity, position,email
            # ,website,bitcoinaddress,trust,dateregistered
            user_form = table_list[7]
            if "'" in user_form.find("b", text=re.compile(
                    "Name")).parent.next_sibling.next_sibling.string:
                user_name = user_form.find("b", text=re.compile(
                    "Name")).parent.next_sibling.next_sibling.string
                user_name = user_name.replace("'", "")
            else:
                user_name = user_form.find("b", text=re.compile(
                    "Name")).parent.next_sibling.next_sibling.string
            user_posts = user_form.find("b", text=re.compile(
                "Posts")).parent.next_sibling.next_sibling.string
            user_activity = user_form.find("b", text=re.compile(
                "Activity")).parent.next_sibling.next_sibling.string
            user_position = user_form.find("b", text=re.compile(
                "Position")).parent.next_sibling.next_sibling.string
            user_gender = user_form.find("b", text=re.compile(
                "Gender")).parent.next_sibling.next_sibling.string
            if user_form.find("b", text=re.compile("Age")).parent.next_sibling.next_sibling.string == 'N/A':
                user_age = None
            else:
                user_age = user_form.find("b", text=re.compile(
                    "Age")).parent.next_sibling.next_sibling.string
            if user_form.find("b", text=re.compile(
                    "Location")).parent.next_sibling.next_sibling.string is not None:
                pass
                user_location = user_form.find("b", text=re.compile(
                    "Location")).parent.next_sibling.next_sibling.string.strip()
            else:
                user_location = None
            if user_form.find("b", text=re.compile(
                    "Email")).parent.next_sibling.next_sibling.string is not None:
                user_email = user_form.find("b", text=re.compile(
                    "Email")).parent.next_sibling.next_sibling.string.strip()
            else:
                user_email = None
            if user_form.find("b", text=re.compile(
                    "Website")).parent.next_sibling.next_sibling.string is not None:
                user_website = user_form.find("b", text=re.compile(
                    "Website")).parent.next_sibling.next_sibling.string.strip()
            else:
                user_website = None
            user_trust = user_form.find(
                "a", text="Trust").parent.parent.next_sibling.next_sibling.b.string
            user_dateregistered = user_form.find("b", text=re.compile(
                "Date Registered")).parent.next_sibling.next_sibling.string
            if user_form.find("b", text=re.compile("Bitcoin address")) is None:
                user_bitcoinaddress = None
            else:
                user_bitcoinaddress = user_form.find("b", text=re.compile(
                    "Bitcoin address")).parent.next_sibling.next_sibling.string
            user = user_info(user_name, user_posts, user_gender, user_age, user_location, user_activity,
                             user_position, user_email, user_website, user_bitcoinaddress,
                             user_trust, user_dateregistered)
            print user_name.encode('gbk', 'ignore')
            print user_posts
            print user_activity
            print user_position
            print user_gender
            print user_age
            if user_location is not None:
                print user_location.encode('gbk', 'ignore')
            else:
                print user_location
            if user_email is not None:
                print user_email.encode('gbk', 'ignore')
            else:
                print user_email
            if user_website is not None:
                print user_website.encode('gbk', 'ignore')
            else:
                print user_website
            print user_trust
            print user_dateregistered
            print user_bitcoinaddress
            insert_user(user)
            user_url_list.remove(url)
            if len(user_url_list):
                strhref = "--".join(user_url_list)
                save_subject_file('user_url_list' + str(sort_num) + '.txt', strhref)
                count += 1
                print "`````````````````````" + str(count) + "````````````````````````````````"
            else:
                clear_file('user_url_list' + str(sort_num) + '.txt')

    else:
        print 'no user can be insert'
        return None

# 类层———————————————————————————————————————————class———————————————————————————————————————————————


# 话题类


class topic_info:

    def __init__(self, topic_id, topic_name, topic_host, topic_board, topic_replies, topic_views, topic_href):
        self.topic_id = topic_id
        self.topic_name = topic_name
        self.topic_host = topic_host
        self.topic_board = topic_board
        self.topic_replies = topic_replies
        self.topic_views = topic_views
        self.topic_href = topic_href

# 消息类


class message_info:
    """docstring for message_info"""

    def __init__(self, id, host, date, topic, quote, reply):
        self.message_id = id
        self.message_host = host
        self.message_date = date
        self.message_topic = topic
        self.message_quote = quote
        self.message_reply = reply
# 用户信息表


class user_info:
    # user

    def __init__(self, name, posts, gender, age, location, activity, position, email, website, bitcoinaddress, trust, dateregistered):

        self.user_name = name
        self.user_posts = posts
        self.user_gender = gender
        self.user_age = age
        self.user_location = location
        self.user_activity = activity
        self.user_position = position
        self.user_email = email
        self.user_website = website
        self.user_bitcoinaddress = bitcoinaddress
        self.user_trust = trust
        self.user_dateregistered = dateregistered

    def displayCount(self):
        print "Total user %d" % user_info.user_Count

    def displayuser(self):
        print "Name : " + str(self.user_name) + ",activity: " + str(self.user_activity)

if __name__ == '__main__':
    # get_topic_url_list()
    # parse_user_form()
    # insert_user()
    # creattable()
    # insert_message()
    # insert_topic()
    # delete_table("topic_info")
    # get_post_soup()
    # get_onepage_poster_url()
    # get_onepage_message_info()
    # delete_table("user_info")
    # delete_table("message_info")

    # select_topic_inDB(1)
    use_proxy()
    if parse_user_info_from_topic(2):
        parse_message_info_from_topic(2)
    print "this 100 topic parse complete!"
    # parse_onetopic_messageinfo('https://bitcointalk.org/index.php?topic=2059111.0')
