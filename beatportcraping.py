from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
from selenium import webdriver
from selenium import common
from selenium.webdriver.common.by import By
import time
import numpy as np 

import asyncio
import time
import aiohttp

DRIVER_PATH = '/home/broomrider/chromedriver/chromedriver'

headers = {"User-Agent": 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.146 Safari/537.36'}

releases = []
keys = []
bpms = []
genres = []

# driver = webdriver.Chrome(executable_path=DRIVER_PATH)

async def scrap_all_songs(sites):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in sites:
            task = asyncio.ensure_future(parse_content(session, url))
            tasks.append(task)
            await  asyncio.sleep(2)    
        await asyncio.gather(*tasks, return_exceptions=True)
      
async def parse_content(session, url):
    if url is not np.nan and url.split('+')[-1] != 'ID':
        url += '+beatport'
        async with session.get(url, headers = headers) as response:
            print(response)
            try:
                pattern = re.compile(r'(span class=\"st\".*\bBPM\b\s\d\d\d.*?)>')
                rtext = re.search(pattern, response)
                text = rtext.group().split('span class="st"')[-1]
            except:
                released = np.nan
                bpm = np.nan
                key = np.nan
                genre = np.nan  
            
            else:
                try:
                    released = text.split('Released')[1].split(';')[0]
                except:
                    released = np.nan
                
                try:
                    bpm = text.split('BPM')[1].split(';')[0]
                except:
                    bpm = np.nan
                
                try:
                    key = text.split('Key')[1].split(';')[0]
                except:
                    key = np.nan
                
                try:
                    genre = text.split('Genre')[1].split(';')[0]
                except:
                    genre = np.nan

            finally:
                releases.append(released)
                keys.append(key)
                bpms.append(bpm)
                genres.append(genre)
    else:
        releases.append(np.nan)
        bpms.append(np.nan)
        keys.append(np.nan)
        genres.append(np.nan)  

if __name__ == "__main__":
    
    df = pd.read_csv('./tracklist.csv')
    df.replace('Nan', np.nan, inplace=True)
    df.dropna(subset=['Artist','TrackName'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    asyncio.get_event_loop().run_until_complete(scrap_all_songs(df['Google link']))
    
    df['Released'] = releases
    df['BPM'] = bpms
    df['Key'] = keys
    df['Genre'] = genres
    df.to_csv('_ftracklist.csv', sep=',', index=False)