#!/usr/bin/env python
# -!- coding:UTF-8 -!-

import re
import pymysql
import requests
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

matplotlib.use('TkAgg')

URL = 'http://kaijiang.500.com/static/info/kaijiang/xml/ssq/list.xml?_A=BLWXUIYA1546584359929'
PROXIES = {
    "http": "http://10.192.30.188:63128",
    "https": "http://10.192.30.188:63128",
}
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}
SQL_CONNECT = {'host': '127.0.0.1',
               'user': 'root',
               'password': '12345678',
               'database': 'markutan',
               'table_name': 'ssq_info'}
reds_count = []
blues_count = []


def get_data_from_url():
    reponse = requests.get(url=URL, headers=HEADERS, proxies=PROXIES)
    pattern = re.compile(r'<row.*?opencode="(.*?)".*?opentime="(.*?)"')
    return pattern.findall(reponse.text)


def write_to_mysql():
    connection = pymysql.connect(SQL_CONNECT['host'],
                                 SQL_CONNECT['user'],
                                 SQL_CONNECT['password'],
                                 SQL_CONNECT['database'])
    cursor = connection.cursor()

    cursor.execute("drop table if exists " + SQL_CONNECT['table_name'])
    sql_create_table = """create table markutan.ssq_info (
                                         id  int(0) not null primary key auto_increment,
                                         red1  char(20) not null,
                                         red2  char(20) not null,
                                         red3  char(20) not null,
                                         red4 char(20) not null,
                                         red5 char(20) not null,
                                         red6 char(20) not null,
                                         blue  char(20) not null,
                                         date  datetime not null)"""

    cursor.execute(sql_create_table)
    ssq_data = get_data_from_url()

    for data in ssq_data:
        ball_info, date_info = data
        red, blue = ball_info.split("|")
        red1, red2, red3, red4, red5, red6 = red.split(",")
        sql_insert = 'insert into ' + \
                       SQL_CONNECT['table_name'] + \
                       '(red1,red2,red3,red4,red5,red6,blue,date) values(%s,%s,%s,%s,%s,%s,%s,%s)'
        try:
            cursor.execute(sql_insert, [red1, red2, red3, red4, red5, red6, blue, date_info])
            connection.commit()
        except Exception as e:
            print("write data to mysql error: {}".format(e))
            connection.rollback()


def get_data_from_mysql():
    connection = pymysql.connect(SQL_CONNECT['host'],
                                 SQL_CONNECT['user'],
                                 SQL_CONNECT['password'],
                                 SQL_CONNECT['database'])
    cursor = connection.cursor()
    cursor.execute('select * from ' + SQL_CONNECT['table_name'])
    results = cursor.fetchall()
    # print("results:\n{}".format(results))

    blues = []
    reds = []
    for row in results:
        # print("row:\n{} {}".format(row, row[2]))
        blue = row[7]
        red_list = [row[1], row[2], row[3], row[4], row[5], row[6]]

        reds.extend(red_list)
        blues.append(blue)

    # print("reds: {}".format(reds))
    # print("blues: {}".format(blues))

    global reds_count, blues_count
    for i in range(1, 34): reds_count.append(reds.count(str(i).zfill(2)))
    for i in range(1, 17): blues_count.append(blues.count(str(i).zfill(2)))
    # print("reds_count: \n{}".format(reds_count))
    # print("blues_count: \n{}".format(blues_count))


def auto_label(rects):
    for rect in rects:
        height = rect.get_height()
        plt.text(rect.get_x()-rect.get_width()/4, 1.02*height, "%s" % int(height))


def red_statistics():
    width = 0.35
    index = np.arange(1, 34)
    y = reds_count
    y1 = np.array(y)
    x1 = index + 1
    # print("x1:{} y1:{}".format(x1, y1))
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    rect = ax1.bar(x1, y1, width, facecolor='#9999ff', edgecolor='white')
    x = [str(i) for i in range(1, 34)]
    plt.xticks(index+1+width/20, x)
    plt.ylim(0, 500)
    auto_label(rect)
    ax1.xaxis.set_ticks_position('bottom')
    l1 = ax1.legend(loc=(.02,.92), fontsize=16)
    plt.show()


def blue_statistics():
    width = 0.35
    index = np.arange(1, 17)
    y = blues_count
    y1 = np.array(y)
    x1 = index + 1
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    rect = ax1.bar(x1, y1, width, facecolor='#9999ff', edgecolor='white')
    x = [str(i) for i in range(1, 17)]
    plt.xticks(index + 1 + width / 20, x)
    plt.ylim(0, 500)
    auto_label(rect)

    ax1.xaxis.set_ticks_position('bottom')
    l1 = ax1.legend(loc=(.02, .92), fontsize = 16)
    plt.show()


if __name__ == '__main__':
    write_to_mysql()
    get_data_from_mysql()
    red_statistics()
    blue_statistics()


