#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：sechat
@File    ：SEchat.py
@Author  ：redcomet97@outlook.com
@Date    ：2022.2.12
'''

import sqlite3
import time
import datetime
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import re
import random
import jieba
import jieba.posseg
import collections
from PIL import Image
import os
from wordcloud.wordcloud import WordCloud
import warnings

warnings.filterwarnings('ignore')


class weChatAnalysis():
    def __init__(self):
        self.all_data = None
        self.sticker_count = None
        self.En2Cn = {
            'a': '形容词',
            'ad': '形容词-副形词',
            'ag': '形容词-形容词性语素',
            'al': '形容词-形容词性惯用语',
            'an': '形容词-名形词',
            'b': '区别词',
            'bl': '区别词-区别词性惯用语',
            'c': '连词',
            'cc': '连词-并列连词',
            'd': '副词',
            'e': '叹词',
            'eng': '英文',
            'f': '方位词',
            'g': '语素',
            'h': '前缀',
            'i': '成语',
            'j': '简称略语',
            'k': '后缀',
            'l': '习用语',
            'm': '数词',
            'mq': '数量词',
            'n': '名词',
            'ng': '名词-名词性语素',
            'nl': '名词-名词性惯用语',
            'nr': '名词-人名',
            'nr1': '名词-汉语姓氏',
            'nr2': '名词-汉语名字',
            'nrf': '名词-音译人名',
            'nrfg': '名词-人名',
            'nrj': '名词-日语人名',
            'ns': '名词-地名',
            'nsf': '名词-音译地名',
            'nt': '名词-机构团体名',
            'nz': '名词-其他专名',
            'o': '拟声词',
            'p': '介词',
            'pba': '介词-“把”',
            'pbei': '介词-“被”',
            'q': '量词',
            'qt': '量词-动量词',
            'qv': '量词-时量词',
            'r': '代词',
            'rg': '代词-代词性语素',
            'rr': '代词-人称代词',
            'rz': '代词-指示代词',
            'rzs': '代词-处所指示代词',
            'rzt': '代词-时间指示代词',
            'rzv': '代词-谓词性指示代词',
            'ry': '代词-疑问代词',
            'rys': '代词-处所疑问代词',
            'ryt': '代词-时间疑问代词',
            'ryv': '代词-谓词性疑问代词',
            's': '处所词',
            't': '时间词',
            'tg': '时间词-时间词性语素',
            'u': '助词',
            'ude1': '助词-“的”“底”',
            'ude2': '助词-“地”',
            'ude3': '助词-“得”',
            'udeng': '助词-“等”“等等”“云云”',
            'udh': '助词-“的话”',
            'uguo': '助词-“过”',
            'ule': '助词-“了”“喽”',
            'ulian': '助词-“连”',
            'uls': '助词-“来讲”“来说”“而言”“说来”',
            'usuo': '助词-“所”',
            'uyy': '助词-“一样”“一般”“似的”“般”',
            'uzhe': '助词-“着”',
            'uzhi': '助词-“之”',
            'v': '动词',
            'vd': '动词-副动词',
            'vf': '动词-趋向动词',
            'vg': '动词-动词性语素',
            'vi': '动词-不及物动词（内动词）',
            'vl': '动词-动词性惯用语',
            'vn': '动词-名动词',
            'vshi': '动词-“是”',
            'vx': '动词-形式动词',
            'vyou': '动词-“有”',
            'w': '标点符号',
            'wb': '标点符号-百分号千分号，全角：％ ‰ 半角：%',
            'wd': '标点符号-逗号，全角：， 半角：,',
            'wf': '标点符号-分号，全角：； 半角： ; ',
            'wj': '标点符号-句号，全角：。',
            'wh': '标点符号-单位符号，全角：￥ ＄ ￡ ° ℃ 半角 $',
            'wkz': '标点符号-左括号，全角：（ 〔 ［ ｛ 《 【 〖 〈 半角：( [ { <',
            'wky': '标点符号-右括号，全角：） 〕 ］ ｝ 》 】 〗 〉 半角： ) ] { >',
            'wm': '标点符号-冒号，全角：： 半角： :',
            'wn': '标点符号-顿号，全角：、',
            'wp': '标点符号-破折号，全角：—— －－ ——－ 半角：—',
            'ws': '标点符号-省略号，全角：…… …',
            'wt': '标点符号-叹号，全角：！ 半角：!',
            'ww': '标点符号-问号，全角：？ 半角：?',
            'wyz': '标点符号-左引号，全角：“ ‘ 『',
            'wyy': '标点符号-右引号，全角：” ’ 』',
            'x': '字符串',
            'xu': '字符串-网址URL',
            'xx': '字符串-非语素字',
            'y': '语气词',
            'z': '状态词',
            'un': '未知词',
            'zg': '未知词',
            'nrt': '未知词',
            'df': '未知词'
        }

    def unix_time(self, dt):
        timeArray = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
        timestamp = int(time.mktime(timeArray)) * 1000
        return timestamp

    def local_time(self, timeNum):
        timeStamp = float(timeNum / 1000)
        timeArray = time.localtime(timeStamp)
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        return otherStyleTime

    def get_tables(self, path):
        mydb = sqlite3.connect(path)
        cursor = mydb.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        table_list = []
        for table in tables:
            table_name = table[0]
            if table_name.startswith('Chat_'):
                table_list.append(table_name)
        cursor.close()
        mydb.close()
        return table_list

    def read_tables(self, path):
        tables = self.get_tables(path)
        print
        mydb = sqlite3.connect(path)
        cursor = mydb.cursor()
        whole_data = pd.DataFrame()
        for table in tables:
            sql = "SELECT * FROM {0};".format(table)
            values = cursor.execute(sql)
            df = pd.DataFrame(data=values)
            df['table_name'] = table
            whole_data = pd.concat([whole_data, df])
        #         print('complete extracting data from {0}'.format(table))
        whole_data.columns = ['create_time', 'des', 'imgstatus', 'meslocalid', 'message', 'messvrid', 'status',
                              'tablever', 'type', 'table_name']
        whole_data['create_time'] = [self.local_time(i) for i in whole_data['create_time'] * 1000]
        return whole_data

    def generate_data(self):
        all_data = pd.DataFrame()
        for file in os.listdir():
            pattern = re.compile(r'message[\s\S]*.sqlite')
            if re.match(pattern, file):
                path = './' + file
                msg_df = self.read_tables(path)
                all_data = pd.concat([all_data, msg_df])
        if all_data.shape[0] == 0:
            print('Cannot find any data')
        else:
            print('There are {0} records found'.format(all_data.shape[0]))
            self.all_data = all_data
        return all_data

    #  Use this function to get the table name you need
    def keyword_to_table(self, all_data, key_word):
        table_name = all_data[all_data['message'].str.contains(key_word)]['table_name'].drop_duplicates()
        return table_name

    def find_target(self, table_name):
        target = self.all_data[self.all_data['table_name'] == table_name].reset_index(drop=True)
        target['year'] = [int(datetime.datetime.strptime(i, "%Y-%m-%d %H:%M:%S").year) for i in target['create_time']]
        target['month'] = [int(datetime.datetime.strptime(i, "%Y-%m-%d %H:%M:%S").month) for i in target['create_time']]
        target['day'] = [int(datetime.datetime.strptime(i, "%Y-%m-%d %H:%M:%S").day) for i in target['create_time']]
        target['hour'] = [int(datetime.datetime.strptime(i, "%Y-%m-%d %H:%M:%S").hour) for i in target['create_time']]
        target['date'] = [int(datetime.datetime.strftime(datetime.datetime.strptime(i, "%Y-%m-%d %H:%M:%S"), "%Y%m%d"))
                          for i in target['create_time']]
        target = target[['create_time', 'date', 'year', 'month',
                         'day', 'hour', 'des', 'imgstatus', 'meslocalid', 'message', 'messvrid',
                         'status', 'tablever', 'type']]
        return target

    def get_group_role(self, group_chat_table):
        character_list = []
        pattern = re.compile(r'wxid_[\s\S]*:')
        for line in group_chat_table['message']:
            try:
                character = re.findall(pattern, line)[0]
                if (len(character) <= 20) & (len(character) > 0):
                    character_list.append(character[:-1])
            except:
                continue
        character_list = list(set(character_list))
        return character_list

    def add_role(self, group_chat_table):
        character_list = self.get_group_role(group_chat_table)
        print('There are {0} people in the group'.format(len(character_list) + 1))
        char = []
        for i, row in group_chat_table.iterrows():
            message = row['message']
            des = row['des']
            for role in character_list:
                if message.startswith(str(role + ':')):
                    curr = role
                    break
                elif des == 0:
                    curr = 'me'
                    break
                else:
                    curr = 'unknown'
            char.append(curr)
        group_chat_table['role'] = char
        return group_chat_table

    def date_line(self, personal_chat):
        date_line_2 = pd.DataFrame(personal_chat.groupby(['date', 'des']).count()['message']).reset_index()
        date_line_2['char'] = ['Y' if i == 1 else 'M' for i in date_line_2['des']]
        date_line_2['message'] = date_line_2['message'].fillna(0)
        date_line_sum = pd.DataFrame(personal_chat.groupby(['date']).count()['message']).reset_index()
        date_line_sum['char'] = 'total'
        date_line_sum['des'] = 3
        date_line_2 = pd.concat([date_line_2, date_line_sum])
        date_line_2['date'] = [datetime.datetime.strptime(str(i), "%Y%m%d") for i in date_line_2['date']]
        date_line_2 = date_line_2.reset_index(drop=True)
        # date_line['date'] = [datetime.datetime.strptime(str(i),"%Y%m%d") for i in date_line['date']]
        plt.figure(figsize=(10, 5))
        sns.set_theme(style="darkgrid")
        palette = sns.color_palette("crest", 3)
        sns.lineplot(x='date', y='message', hue='char', size='des', data=date_line_2, palette=palette)
        plt.savefig('./date.png', transparent=True)

    def group_date_line(self, group_chat):
        date_line_sum = pd.DataFrame(group_chat.groupby(['date']).count()['message']).reset_index()
        date_line_sum['role'] = 'total'
        date_line_2 = date_line_sum
        date_line_2['date'] = [datetime.datetime.strptime(str(i), "%Y%m%d") for i in date_line_2['date']]
        date_line_2 = date_line_2.reset_index(drop=True)
        # date_line['date'] = [datetime.datetime.strptime(str(i),"%Y%m%d") for i in date_line['date']]
        plt.figure(figsize=(10, 5))
        sns.set_theme(style="darkgrid")
        palette = sns.color_palette("crest", 3)
        sns.lineplot(x='date', y='message', data=date_line_2, palette=palette)
        plt.savefig('./group_date.png', transparent=True)

    def group_hour_line(self, group_chat):
        hour_line = pd.DataFrame(group_chat.groupby(['hour', 'role']).count()['message']).reset_index()
        hour_line = hour_line.reset_index(drop=True)
        plt.figure(figsize=(10, 5))
        sns.set_theme(style="darkgrid")
        palette = sns.color_palette("mako")
        sns.barplot(x='hour', y='message', ci=None, data=hour_line, hue='role', palette=palette)
        plt.savefig('./group_hour.png', transparent=True)

    def hour_line(self, personal_chat):
        hour_line = pd.DataFrame(personal_chat.groupby(['hour', 'des']).count()['message']).reset_index()
        hour_line['char'] = ['Y' if i == 1 else 'M' for i in hour_line['des']]
        hour_line_sum = pd.DataFrame(personal_chat.groupby(['hour']).count()['message']).reset_index()
        hour_line_sum['char'] = 'total'
        hour_line_sum['des'] = 3
        hour_line = pd.concat([hour_line, hour_line_sum])
        hour_line = hour_line.reset_index(drop=True)
        plt.figure(figsize=(10, 5))
        sns.set_theme(style="darkgrid")
        palette = sns.color_palette("mako")
        sns.barplot(x='hour', y='message', ci=None, data=hour_line[hour_line['char'] != 'total'], hue='char',
                    palette=palette)
        plt.savefig('./hour.png', transparent=True)

    def msg_type(self, df):
        type_df = pd.DataFrame(df.groupby('type').count()['message']).reset_index()
        mapping_df = pd.DataFrame({'idx': [1, 3, 34, 43, 47, 48, 49, 50, 62, 10000, 10002],
                                   'label': ['text', 'image', 'voice', 'video', 'emoji', 'location', 'quotation',
                                             'voicecall', 'videomsg longer', 'you_recall', 'i_recall']})
        type_df = pd.merge(type_df, mapping_df, how='left', left_on='type', right_on='idx')
        type_df = type_df[['label', 'message']]
        return type_df

    def msg_count(self, chat, type=0):
        # type == 1: group_chat
        if type == 0:
            me = chat.groupby('des').count()['message'][0]
            you = chat.groupby('des').count()['message'][1]
            print('I sent {0} messages'.format(me))
            print('You sent {0} messages'.format(you))
        else:
            df = pd.DataFrame(chat.groupby('role').count()['message']).reset_index()
            for i, row in df.iterrows():
                print("{0} sent {1} messages".format(row['role'], row['message']))

    def generate_wordcloud(self, df, top_n, min_count):
        #     清洗文本数据
        all_words = []
        for string_data in df['message']:
            if string_data.startswith('<msg><appmsg'):
                continue
            else:
                pattern = re.compile(u'\t|\n|\.|-|:|;|\)|\(|\?|"')
                punc = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
                for ele in string_data:
                    if ele in punc:
                        string_data = string_data.replace(ele, "")
                string_data = re.sub('＂＃＄％＆＇（）＊＋，－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､　、〃〈〉《》「」『』【】〔〕〖〗〘〙〚〛〜〝〞〟〾〿–—‘’‛“”„‟…‧﹏﹑﹔·！？｡。',
                                     '', string_data)
                string_data = re.sub('[0-9]', '', string_data)
                string_data = re.sub('[a-zA-Z]', '', string_data)
                string_data = re.sub(pattern, '', string_data)
                # jieba.suggest_freq('大魔王', True)
                seg_list_exact = jieba.cut(string_data, cut_all=False, HMM=True)
                object_list = []
                with open('hit_stopwords.txt', 'r', encoding='UTF-8') as meaninglessFile:
                    stopwords = set(meaninglessFile.read().split('\n'))
                    stopwords.add(' ')
                for word in seg_list_exact:
                    if word not in stopwords:
                        all_words.append(word)

        word_counts = collections.Counter(all_words)
        word_counts_top = word_counts.most_common(100)

        result = pd.DataFrame()
        count = 0
        for TopWord, Frequency in word_counts_top:  # 获取词语和词频
            for POS in jieba.posseg.cut(TopWord):  # 获取词性
                if count == 100:
                    break
                try:
                    flag = [list(self.En2Cn.values())[list(self.En2Cn.keys()).index(POS.flag)]]
                except:
                    flag = 'unknown'
                res = pd.DataFrame({'word': [TopWord], 'count': [int(Frequency)], 'flag': flag})
                result = pd.concat([result, res])
                count += 1

        result = result[[len(word) >= 2 for word in result['word']]].reset_index(drop=True)  # 移除两个字以下的词语
        #     只保留特定词性
        #     移除数量小于 min_n的词语
        result = result[(result['flag'] != '时间词') & (result['flag'] != '习用语') & (result['flag'] != '代词')
                        & (result['flag'] != '形容词-副形词') & (result['flag'] != '处所词')
                        & (result['count'] >= min_count)].reset_index(drop=True)
        top_word = result.drop_duplicates('word')[0:top_n].reset_index()
        res = []
        for i in range(top_word.shape[0]):
            res.append((top_word['word'][i], top_word['count'][i]))
        res = dict(res)
        #     生成词云并保留
        font = r'C:\\Windows\\fonts\\simyou.ttf'
        my_wordcloud = WordCloud(
            #                              background_color=None,
            font_path=font,
            background_color='white',
            max_font_size=160, random_state=30).generate_from_frequencies(res)
        # plt.imshow(my_wordcloud)
        plt.axis("off")
        plt.savefig('./wordcloud.png', transparent=True)
        # plt.show()
        return plt

    # view-source:https://emojipedia.org/wechat/
    # use this source code to download all the wechat emojis to local
    #  remember to copy and paster the emoji png source to emoji_text.text
    # def get_wechat_emoji():
    #     with open('C:\\Users\\Lenovo\\Downloads\\emoji_text.txt') as f:
    #         lines = f.readlines()
    #     pattern = re.compile(r'<td>\[[\s\S]*\]</td>')
    #     emoji_name = re.findall(pattern, lines[0])
    #     emoji_names = []
    #     for line in lines:
    #         emoji_name = re.findall(pattern, line)
    #         if len(emoji_name) > 0:
    #             emoji_names.append(emoji_name)
    #     for line in lines:
    #         emoji_name = re.findall(pattern, line)
    #         if len(emoji_name) > 0:
    #             emoji_names.append(emoji_name)
    #     pattern = re.compile(r'src="[\s\S]* width=')
    #     emoji_srcs = []
    #     for line in lines:
    #         emoji_src = re.findall(pattern, line)
    #         if len(emoji_src) > 0:
    #             emoji_srcs.append(emoji_src)
    #     emoji_src = []
    #     for emoji in emoji_srcs:
    #         emoji = emoji[0][5:]
    #         emoji = emoji[:-8]
    #         emoji_src.append(emoji)
    #     from urllib.request import urlretrieve
    #     for i in range(len(emoji_name)):
    #         urlretrieve(emoji_src[2 * i + 1], './wechat_emoji/{0}.png'.format(emoji_name[i]))
    def stickers_search(self, df):
        res = []
        for string in df['message']:
            pattern = re.compile(r'\[[A-Z][a-z]+\]')
            stickers = re.findall(pattern, string)

            if len(stickers) > 0:
                for sticker in stickers:
                    res.append(sticker)
        res = pd.value_counts(res)
        res = pd.DataFrame(res).reset_index()
        res.columns = ['emoji', 'count']
        res['count'] = [int(np.log(i) * 45) for i in res['count']]
        self.sticker_count = res
        return res

    def generate_emojicloud(self, table):
        self.stickers_search(table)
        sticker_count = self.sticker_count
        sticker_count['count'][0] = 250
        sticker_count['count'][1] = 200
        sticker_count['count'][2] = 180
        sticker_count['count'][3] = 160
        sticker_count['count'][4] = 150
        sticker_count['count'][5] = 140
        sticker_count['count'][6] = 130
        sticker_count['count'][7] = 120
        sticker_count['count'][8] = 110
        sticker_count['count'][9] = 100
        sticker_count['count'][10:15] = 75
        sticker_count['count'][15:20] = 50
        loc_dict = {'0': (250, 225),
                    '1': (500, 100),
                    '2': (480, 450),
                    '3': (150, 500),
                    '4': (130, 110),
                    '5': (520, 300),
                    '6': (350, 100),
                    '7': (100, 380),
                    '8': (350, 500),
                    '9': (100, 280)}
        dst = Image.new('RGBA', (800, 800))
        for i in range(20):
            try:
                emoji_name = sticker_count['emoji'][i][1:]
                emoji_name = emoji_name[:-1]
                emoji_size = sticker_count['count'][i]
                emoji_picture = Image.open("./wechat_emoji/{0}.png".format(str(emoji_name)))
                emoji_picture = emoji_picture.resize((emoji_size, emoji_size), Image.ANTIALIAS)
                emoji_picture = emoji_picture.rotate(random.randint(-10, 10))
                dst.paste(emoji_picture, loc_dict[str(i)])
            except:
                continue
        dst.save('./sticker_cloud.png')
        return dst


if __name__ == "__main__":
    SEdata = weChatAnalysis()
    all_data = SEdata.generate_data()
    print(all_data.shape)

    # key_word1 = '刘源'
    # key_word2 = '刘媛媛'
    # print(SEdata.keyword_to_table(all_data, key_word1)) #找到带有刘源关键词的表名
    # print(SEdata.keyword_to_table(all_data, key_word2)) #找到带有刘媛媛关键词的表名

    group_table = SEdata.find_target('Chat_8542b3e4631c6cca32ad1138cde214da')
    group_table = SEdata.add_role(group_table)
    SEdata.group_date_line(group_table)
    SEdata.group_hour_line(group_table)

    personal_table = SEdata.find_target('Chat_3a64a2fbf5745f322bac47de87518aec')
    SEdata.date_line(personal_table)
    SEdata.hour_line(personal_table)

    SEdata.msg_count(personal_table, 0)
    SEdata.msg_count(group_table, 1)

    type_df = SEdata.msg_type(personal_table)
    word_cloud = SEdata.generate_wordcloud(personal_table, 25, 21)
    sticker_cloud = SEdata.generate_emojicloud(personal_table)
