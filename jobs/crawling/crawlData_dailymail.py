# import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import json
import random
import os
import stat

league_to_scrap = ["premierleague", "bundesliga", "ligue-1", 
                   "serie-a", "la-liga"]

class Scraping:
    def __init__(self, main_url):
        self.url = main_url

    def get_driver(self):
        try:
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

            user_agent = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15'
            ]
            ua = random.choice(user_agent)
            options.add_argument(f"user-agent={ua}")

            driver_path = "/usr/local/bin/chromedriver"
            if not os.access(driver_path, os.X_OK):
                print("Making chromedriver executable")
                st = os.stat(driver_path)
                os.chmod(driver_path, st.st_mode | stat.S_IEXEC)
            driver = webdriver.Chrome(service=Service(driver_path), options=options)
            print("Driver created")
            return driver

        except Exception as e:
            print(f"[ERROR] Failed to create driver: {e}")
    
    def get_news(self):
        driver = self.get_driver()
        for league in league_to_scrap:
            all_href = []
            full_url = f"{self.url}{league}"
            driver.get(full_url)
            time.sleep(5)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            time.sleep(1)
            main_article = soup.find('div', class_="mainArticle--F664X article").find('a')
            main_article_url = main_article['href']
            print(main_article_url)
            all_href.append(main_article_url)
            body_article =soup.find('ul', class_="articles--QgaHc")
            time.sleep(2)
            body_article_ul = body_article.find_all('li', class_='article--fI6DK')
            for idx, news in enumerate(body_article_ul):
                if idx >= 15:
                    break
                current_check = news.find('h2', 'linkro-darkred').find('a')
                if current_check:
                    current_check_url = current_check['href']
                    print(current_check_url)
                    all_href.append(current_check_url)
                else:
                    print("lỗi")
            news_list = []
            for news in all_href:
                driver.get(news)
                time.sleep(4)
                soup2 = BeautifulSoup(driver.page_source, 'html.parser')
                headlines = soup2.find('div', {'id':'js-article-text'}).find('h1').text.strip()
                time_published_button = soup2.find('span', class_='article-timestamp article-timestamp-published').find('time')
                time_published = time_published_button['datetime']
                tag_button = soup2.find('div', class_='linkButtonRow articleTopicsRow').find_all('a')
                tag_list = []
                for tag in tag_button:
                    tag_list.append(tag.text.strip())
                this_data = {
                    "headline":headlines,
                    "published":time_published,
                    "url": news,
                    "categories":tag_list
                }
                print(this_data)
                news_list.append(this_data)
            with open(f"/app/output/news/dailymail/{league}_dailymailnews.json", "w", encoding="utf-8") as f:
                    json.dump(news_list, f, ensure_ascii=False, indent=2)
            print("đã lưu")

base_url = "https://www.dailymail.co.uk/sport/"
def save_data_all():
    Scraping(base_url).get_news()

save_data_all()