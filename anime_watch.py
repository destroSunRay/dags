from datetime import datetime, timedelta

from airflow.decorators import dag,task

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

default_args={
    'owner':'surya',
    'retries':1,
    'retry_delay':timedelta(seconds=30)
}

@dag(
    description = "dag to read the recent anime form anime-watch website",
    default_args = default_args,
    catchup = False,
    start_date = datetime.strptime('2023-10-25','%Y-%m-%d'),
    schedule_interval = "*/30 * * * *"
)
def recent_anime_releases():

    @task()
    def get_anime_updates():

        URL = 'https://aniwatch.to/recently-updated'
        res = requests.get(URL)
        soup = BeautifulSoup(res.text, "html.parser")
        recent_animes = soup.body.find(class_="film_list-wrap").find_all(class_="flw-item")

        data = []
        for anime in recent_animes:
            try:
                id =int(anime.find('a', class_='film-poster-ahref')['data-id'])
                name = anime.find('a', class_='dynamic-name').text
                sub = anime.find('div',class_='tick-sub')
                sub = int(sub.text) if sub else sub
                dub = anime.find('div',class_='tick-dub')
                dub = int(dub.text) if dub else dub
                description = anime.find('div', class_='description')
                description = description.text.strip() if description else None
                img_src = anime.find('img')
                img_src = img_src['data-src'] if img_src else None

                data.append((id,name,sub,dub,description,img_src))
            except:
                with open("log.txt",'a') as file:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    file.write(f'[{now}] ERROR: unable to extract data from following anime:\n')
                    file.write(f"{str(id)}, {name}\n")
                    file.write(f"{str(id)}:\n {anime}\n\n")
        return data

    @task()
    def update_anime_watch(data):

        import pymysql
        cnx = pymysql.connect(host='localhost',
                            user='surya',
                            password='Asspr2320',
                            port=3306,
                            database='myhpnet')
        with cnx:
            with cnx.cursor() as cursor:
                query="REPLACE INTO anime_watch(id, name, sub, dub, description, images)\
                    VALUES (%s,%s,%s,%s,%s,%s)"
                for d in data:
                    cursor.execute(query,d)
                cnx.commit()


    data = get_anime_updates()
    update_anime_watch(data)

update_recent_anime = recent_anime_releases()
