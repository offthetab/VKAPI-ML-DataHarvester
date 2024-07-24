import requests
import json
import csv
import datetime
import time
import os
from typing import Union
from math import ceil
import re

BASE_URL = 'https://api.vk.ru/method/'

domain = 'kafprimat'
group_id = 189112841
access_token = '530ae03e530ae03e530ae03e54501e232e5530a530ae03e3797245500573db178d49233'    # add your valid api token here лень делать .env
version = '5.236'

def text_cleaner(text):
    RE_EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)

    text_no_emojis = RE_EMOJI.sub(r'', text)
    text_no_punctuation = re.sub(r'[^\w\s]', '', text_no_emojis)
    text_no_punctuation = re.sub(r'\n', ' ', text_no_punctuation)
    text_no_punctuation = re.sub(r'\t', ' ', text_no_punctuation)
    text_no_punctuation = re.sub(r'\s+', ' ', text_no_punctuation)
    text_no_punctuation = text_no_punctuation.strip()
    return text_no_punctuation

def platform_converter(platform: Union[str, None]):
    dct = {  # словарь возможных значений поля platform метода users.get
        1: 'mobile version',
        2: 'iPhone app',
        3: 'iPad app',
        4: 'Android app',
        5: 'Windows Phone app',
        6: 'Windows 10 app',
        7: 'Full version'}
    return dct.get(platform, None)

sex_conv = {
    0: None,
    1: 0,   # aka female
    2: 1    # aka male
}

def unix_time_converter(unix_time: Union[str, None]) -> str: # функция-конвертер из unix в формат datetime
    if unix_time is not None:
        value = datetime.datetime.fromtimestamp(unix_time)
        return f"{value:%Y-%m-%d %H:%M:%S}"
    return None

def cringe_date_converter(date_: Union[str, None]):
    if date_ is not None:
        tmp = date_.split('.')
        return datetime.date(year=2000, month=int(tmp[1]), day=int(tmp[0])).strftime('%Y-%m-%d')
    return None

def get_age(date_: Union[str, None]):
    if date_ is not None:
        date_ = date_.split('.')
        if len(date_) ==  3:
            days_in_year = 365.2425
            birthDate = datetime.date(year=int(date_[2]), month=int(date_[1]), day=int(date_[0]))   
            age = int((datetime.date.today() - birthDate).days / days_in_year)
            return age
    return None
    
def get_all_users():    # функция получения id всех пользователей группы (можно еще получить какие-то данные о них). Тут попадаются удаленные аккаунты!
    all_user_ids = []

    offset = 0
    while offset < 2000:
        users = requests.get(url=f'{BASE_URL}groups.getMembers', params={
            'group_id': domain,
            'sort': 'id_asc',
            'count': '1000',
            'offset': offset,
            'access_token': access_token,
            'v': version    
        })
        user_ids = users.json()['response']['items'] # все подписчики группы
        offset += 1000
        all_user_ids.extend(user_ids)

    all_user_ids = [str(id) for id in all_user_ids] # id всех пользователей группы 
    return all_user_ids

def get_users_info(user_ids: int) -> list:  # функция получения информации о списке пользователей. На выходе информация о пользователях (нету удаленных аккаунтов)
    user_info = []  

    count = 500
    left = 0
    right = count + 1

    for i in range(ceil(len(user_ids) / count)):
        time.sleep(0.5)
        data = requests.get(url=f'{BASE_URL}users.get', params={
            'user_ids': ','.join(user_ids[left:right]),
            'fields': 'sex,bdate,city,country,verified,last_seen,personal',
            'access_token': access_token,
            'v': version,
            })
        response = data.json()['response']

        result = [[info['id'],
                cringe_date_converter(info.get('bdate', None)),
                get_age(info.get('bdate', None)),
                info.get('city', {'title': None})['title'], 
                info.get('country', {'title': None})['title'], 
                platform_converter(info.get('last_seen', {'platform': None})['platform']), 
                unix_time_converter(info.get('last_seen', {'time': None})['time']),
                sex_conv[info['sex']],
                info['verified'],
                info['first_name'],
                info['last_name'],
                1 if info['is_closed'] == True else 0
                ] for info in response if not 'deactivated' in info]
        
        user_info.extend(result)

        left += count
        right += count
    
    # запись в файл
    with open('users.csv', 'w', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['ID', 'bdate', 'age', 'city', 'country', 'last_seen_platform', 'last_seen_time', 'sex', 'verified', 'first_name', 'last_name', 'is_closed'])
        writer.writerows(user_info)

    return user_info

def get_user_posts(user_id: int) -> list: # Получаем все посты с страницы пользователей
    print(f'get_user_posts {user_id}')
    post_list = []
    offset = 0
    count = 101
    while offset < count:
        time.sleep(0.4)
        data = requests.get(url=f"{BASE_URL}wall.get", params={
            'owner_id': user_id,
            'count': 100,
            'offset': offset,
            'filter': 'owner',
            'access_token': access_token,
            'v': version  
        })

        # private wall handler
        if 'error' in data.json().keys():
            return data.json()
                
        response = data.json()['response']
        
        count = response['count']
        items = response['items']

        result = [[user_id,
                item['id'], 
                unix_time_converter(item['date']),
                item['likes']['count'],
                item['comments']['count'],
                item['reposts']['count'],
                item['views']['count'] if item.get('views', None) is not None else None,
                len(item['attachments']) if item.get('attachments', None) is not None else 0,
                text_cleaner(item['text'])
                ] for item in items if int(unix_time_converter(item['date']).split('-')[0]) >= 2019]    # просмотры на постах появилсись с 2017 года
        post_list.extend(result)

        offset += 100

    with open('user_posts.csv', 'a', encoding='utf-8') as file:
        writer = csv.writer(file)
        if (os.stat('user_posts.csv').st_size == 0): 
            writer.writerow(['user_id', 'post_id', 'date', 'likes', 'comments', 'reposts', 'views', 'attachments', 'text'])
        writer.writerows(post_list)
        
    return post_list

def get_group_posts(): # получаем список постов группы 
    post_list = []
    ids = []
    offset = 0
    count = 1000
    while offset < count:
        time.sleep(0.5)
        data = requests.get(url=f"{BASE_URL}wall.get", params={ 
                'domain': 'kafprimat',
                'count': 100,
                'offset': offset,
                'extended': 1,
                'filter': 'all',
                'access_token': access_token,
                'v': version  
            })
        response = data.json()['response']

        count = response['count']
        items = response['items']

        ids_ = [item['id'] for item in items]

        result = [[item['id'], 
                unix_time_converter(item['date']),
                item['likes']['count'],
                item['comments']['count'],
                item['reposts']['count'],
                item['views']['count'] if item.get('views', None) is not None else None,
                len(item['attachments']) if item.get('attachments', None) is not None else 0,
                len(text_cleaner(item['text']).split(' '))
                ] for item in items]
        post_list.extend(result)
        ids.extend(ids_)
        offset += 100

    # Запись постов в файл
    with open('posts.csv', 'w', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['post_id', 'date', 'likes', 'comments', 'reposts', 'views', 'attachments', 'text'])
        writer.writerows(post_list)

    return ids 

def get_post_likes(post_id: int):# получаем отношение id постов и id пользователей, лайкнушвих пост
    print(f'Обработка поста id: {post_id}')
    time.sleep(0.5)

    data_likes = requests.get(url=f"{BASE_URL}likes.getList", params={
            'type': 'post',
            'owner_id': f'-{group_id}',
            'item_id': post_id,
            'count': 1000,
            'access_token': access_token,
            'v': version  
        })
    try:
        likes_ids = data_likes.json()['response']['items']
    except:
        get_post_likes(post_id=post_id)
        return None

    rows = zip([post_id for i in range(len(likes_ids))], likes_ids)

    with open('post_likes.csv', 'a') as file:
        writer = csv.writer(file)
        if (os.stat('post_likes.csv').st_size == 0): 
            writer.writerow(['post_id', 'user_id'])
        for row in rows:
            writer.writerow(row)

    return None   

def get_post_comments():
    data_comments = requests.get(url=f"{BASE_URL}wall.getComments", params={
            'owner_id': f'-{group_id}',
            'post_id': 2954,
            'count': 100,
            'access_token': access_token,
            'v': version  
        })
    comments = data_comments.json()['response']
    print(comments)
    return None 

def truncate_csvs():
    try:
        os.remove("users.csv")
    except:
        pass        

    try:
        os.remove("posts.csv")
    except:
        pass
    
    try:
        os.remove("post_likes.csv")
    except:
        pass

    try:
        os.remove("user_posts.csv")
    except:
        pass

    return None

if __name__ == "__main__": # на выполнение всего скрипта ушло 31 минута
    truncate_csvs()    # чистим csv файлы
    
    group_user_list = get_all_users()   # получаем id всех участников группы
    
    # posts_ids = get_group_posts()   # тут происходит запись постов в файл, на выходе получаем id всех постов группы

    user_infos = get_users_info(group_user_list)    # получаем личную информацию о пользователях с страницы по id
    
    # [get_post_likes(post_id=post) for post in posts_ids]    # записываем отношение id постов и id пользователей, лайкнувших пост

    # [get_user_posts(user_id=user_id) for user_id in group_user_list]    # получаем посты на личных страницах пользователей



