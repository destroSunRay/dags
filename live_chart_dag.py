import requests
from bs4 import BeautifulSoup
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from airflow.decorators import task, dag

default_args={
    'owner':'surya',
    'retries':1,
    'retry_delay':timedelta(seconds=30)
}

@dag(
    description = "dag to read the recent anime form anime-watch website",
    default_args = default_args,
    catchup = False,
    start_date = datetime.strptime('2024-01-10','%Y-%m-%d'),
    schedule_interval = "0 12 * * *"
)
def collect_data_from_live_chart():

    @task()
    def get_website(season, year):
        headers={
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding':'gzip',
            'Accept-Language':'en-US,en;q=0.9,te;q=0.8',
            'Cache-Control':'no-cache',
            'Cookie':'_ga=GA1.3.1865816974.1657809308; preferences=%7B%22time_zone%22%3A%22America%2FNew_York%22%2C%22titles%22%3A%22romaji%22%7D; lang=%5B%22en-us%22%2C%22en%22%2C%22te%22%5D; cf_clearance=ciqXyh4RrB5k2P46hWN77AAfvFXTaHWoBYLYrAxU2l8-1688253416-0-150; _gid=GA1.3.133754087.1697915088; __Host-livechart_session=lI3nAwfvTq%2BXLhtlg0ZnSy6YPGiBldr%2ByqLX2LYawSfqlRcqig4Os1aX%2BUf7BCxsxyOxEBDxbxiacf8CYV%2FF4ZZoMTugAgHmXT0jh2VWrx5K0wWLppxMEnydmes4rCidwUOJxp%2FsxeoRhoqtWf%2BgZdWejpB6%2Fbo7B4GK4MIN%2Bs4%2BA6WbvpQETtRhahsrVsHjkVdoRBiiSAUBD48GT4ZtHKd0yIvLZLnSVm7VvjC1CiLzzacLUvn6%2BRTuLIKnOpqRppandvpy5%2BfKPgsM3RBXWpGsuPp2RISd2N986eYxnaujZhBRuu31ZLs%2FiqAeaYfeYClmZ4Vr2JKir5pS0YmwHvY2TOgcuL25Gl681dFwANYG2CHKhCMH0RnpSEdyh1%2F5uwlwdoPkwvam3f3HpsVPqANc7HM%3D--RHYtmLMzYquLIrP6--fHoFJ6AZALzMJv%2FSJzHP%2Fw%3D%3D; _ga_B6ETE2XHBJ=GS1.3.1697949028.67.1.1697949053.0.0.0',
            'Pragma':'no-cache',
            'Sec-Ch-Ua-Mobile':'?0',
            'Sec-Ch-Ua-Platform':'"macOS"',
            'Sec-Fetch-Dest':'document',
            'Sec-Fetch-Mode':'navigate',
            'Sec-Fetch-Site':'none',
            'Sec-Fetch-User':'?1',
            'Upgrade-Insecure-Requests':'1',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
        }
        domain = 'https://www.livechart.me'
        URL = f'{domain}/{season}-{year}/tv'
        res = requests.get(URL,headers=headers)
        return res

    @task()
    def select_req_data(res):
        soup = BeautifulSoup(res.text, "html.parser")
        animes = soup.body.main.find_all('article',class_='anime')
        library_entities = soup.body.find_all('script', id='library_entries')
        anime_interests = json.loads(library_entities[0].text)
        return {'animes':animes, 'anime_interests': anime_interests}

    @task()
    def record_error(title, finding, command, value, error, occured_at, season='winter', year='2024'):
        log_path = Path('live_chart_errors.log')

        ttime = occured_at.strftime('%Y-%m-%d %H:%M:%S')

        with log_path.open('a') as file:
            file.write(f"[{ttime}] : error with finding the {finding} for anime('{title}')\n")
            file.write(f"Command : {command}\n")
            file.write(f"Result : {value}\n")
            file.write(f"Error : {error}\n\n")

        with open('live_chart_data.json', 'r') as file:
            data = json.load(file)

        if not data:
            data={}

        data[f'{year}'] = data.get(f'{year}',{})
        ex_data = data[year].get(season,{})
        new_data = {'title': title,
                    'command': command,
                    'result': str(value),
                    'error': str(error),
                    'while_finding': finding}
        ex_data[ttime] = ex_data.get(ttime,[])
        ex_data[ttime].append(new_data)
        data[year][season] = ex_data

        with open('live_chart_data.json','w') as file:
            json.dump(data, file, indent= 4)

    @task()
    def collect_website_data(animes, anime_interests):
        anime_data = {}

        # looping through animes on the website and creating a key-value pair in the anime_data
        now = datetime.now()
        for anime in animes:
            try:
                id = int(anime['data-anime-id'])

                title = anime.h3.a.text

                generes = [tag.text for tag in anime.ol.find_all('li')]

                poster = anime.find('div',class_='poster-container')
                next_ep = int(poster.time['data-label'][2:]) if poster.time else None
                timestamp = poster.time['data-timestamp'] if poster.time else None
                next_ep_datetime = datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M:%S") if timestamp else None
                image = poster.img['src']

                # Rating
                try:
                    rating = float(poster.find('div',class_='anime-extras').text)
                except Exception as e:
                    record_error(title, 'rating', "poster.find('div',class_='anime-extras')", poster.find('div',class_='anime-extras'), e, now)
                    rating = 0

                info = anime.find('div',class_='anime-info')

                studios = [studio.text for studio in info.ul.find_all('li')]
                anime_source = info.find('div',class_='anime-source')

                # Number of Episodes and Anime Duration
                try:
                    anime_eps = info.find('div',class_='anime-episodes').text.strip()
                    if '×' in anime_eps:
                        number_eps = anime_eps.split(' × ')[0].split(' ')[0]
                        number_eps = int(number_eps) if '?' not in number_eps else 0
                        anime_duration = int(anime_eps.split(' × ')[1][:-1])
                    else:
                        number_eps = 1
                        anime_duration = int(anime_eps[:-1])
                except Exception as e:
                    record_error(title, 'number of episodes',
                                        "info.find('div',class_='anime-episodes').text.strip()",
                                        info.find('div',class_='anime-episodes').text.strip(),
                                        e, now)
                    number_eps = None
                    anime_duration = None

                # Summary
                try:
                    summary = '\n'.join([p.text for p in info
                                    .find('div',class_='anime-synopsis')
                                    .find_all('p',class_=None)])
                except Exception as e:
                    summary=''
                    record_error(title, 'summary',
                                        "'\n'.join([p.text for p in info\
                                        .find('div',class_='anime-synopsis')\
                                        .find_all('p',class_=None)])",
                                        info.find('div',class_='anime-synopsis'),
                                        e, now)

                # Information Source
                try:
                    info_source = info.find('div',class_='anime-synopsis')\
                                        .find('p',class_='text-italic').text.split(' ')[1][:-1]
                except Exception as e:
                    record_error(title, 'anime_source',
                                        "info.find('div',class_='anime-synopsis').find('p',class_='text-italic').text.split(' ')[1][:-1]",
                                        info.find('div',class_='anime-synopsis'),
                                        e, now)
                    info_source = None

                anime_data[str(id)]={
                    'title': title,
                    'year': year,
                    'season': season,
                    'generes': generes,
                    'next_ep': next_ep,
                    'next_ep_datetime': next_ep_datetime,
                    'image' : image,
                    'rating' :rating,
                    'number_eps' : number_eps,
                    'summary': summary,
                    'info_source' : info_source,
                    'status': anime_interests[str(id)].get('status',None)
                }


            except Exception as e:
                record_error(title, 'FATAL ERROR',
                                        f"Unexcpected error found while trying to extract data from {title} aime card",
                                        'False',
                                        e, now)

            return anime_data

    @task()
    def record_data(anime_data):
        anime_file = 'anime_data.json'

        with open(anime_file, 'r') as file:
            data = json.load(file)

        if not data:
            data={}
        data |= anime_data

        with open(anime_file, 'w') as file:
            json.dump(data, file, indent=4)

    #@task()
    #def run():
    seasons = ['winter', 'spring','summer','fall']
    for year in range(2000,2024):
        for season in seasons:
            res = get_website(season, year)
            tmp_result = select_req_data(res)
            animes = tmp_result['animes']
            anime_interests = tmp_result['anime_interests']
            anime_data = collect_website_data(animes, anime_interests)
            record_data(anime_data)
            #time.sleep(10)

    #run()

data = collect_data_from_live_chart()
