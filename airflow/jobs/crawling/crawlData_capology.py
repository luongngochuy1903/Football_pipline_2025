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

year_list = {"2024-2025":"24_25"}
league_list = {"/uk/premier-league/":"premier_leagues","/it/serie-a/":"seria",
                "/de/1-bundesliga/":"bundesliga", "/es/la-liga/":"laliga", "/fr/ligue-1/":"ligue 1"}
class Scraping():
    def __init__(self, base_url):
        self.base_url = base_url
    
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

    def crawl_capology_salary(self):
        for league, folder in league_list.items():
            for year, save in year_list.items():
                player_salary = []
                url = f"{self.base_url}{league}salaries/{year}"
                print(url)
                driver = self.get_driver()
                driver.get(url)
                while(True):
                    time.sleep(7)
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    time.sleep(1)
                    league_table = soup.find('table', class_="table table-bordered table-hover").find('tbody')
                    player_list = league_table.find_all('tr')
                    if not player_list:
                        print("không tìm dc")
                    for item in player_list:
                        club_and_player_name = item.find_all('a')
                        player_name = club_and_player_name[0].text.strip()
                        team_name = club_and_player_name[1].text.strip()
                        attribute = item.find_all('td')
                        if year == "2024-2025":
                            gross_per_w = attribute[2].text.strip()
                            gross_per_y = attribute[3].text.strip()
                            signed = attribute[5].text.strip()
                            expiration = attribute[6].text.strip()
                            gross_remaining = attribute[8].text.strip()
                            release_clause = attribute[9].text.strip()
                        else:
                            gross_per_w = attribute[1].text.strip()
                            gross_per_y = attribute[2].text.strip()
                            signed = "None"
                            expiration = "None"
                            gross_remaining = "None"
                            release_clause = "None"
                        this_data = {
                            "player_name":player_name,
                            "team":team_name,
                            "gross/w" : gross_per_w,
                            "gross/y" : gross_per_y,
                            "signed" : signed,
                            "expiration" : expiration,
                            "gross_remaining": gross_remaining,
                            "release_clause" : release_clause,
                        }
                        print(this_data)
                        player_salary.append(this_data)
                    
                    time.sleep(1)
                    current_page_tag = soup.select_one('.pagination li.page-item.active a')
                    if current_page_tag:
                        current_page = int(current_page_tag.text.strip())
                        print(f"Trang hiện tại: {current_page}")
                        next_li_tag = soup.select_one('.pagination li.page-next')
                        if next_li_tag and 'disabled' in next_li_tag.get('class', []):
                            print("Đã đến trang cuối")
                            break

                    # Tìm và click nút Next
                    try:
                        next_btn = driver.find_element(By.CSS_SELECTOR, '.pagination li.page-next a')
                        driver.execute_script("arguments[0].click();", next_btn)
                        time.sleep(3)
                    except Exception as e:
                        print("Không tìm thấy nút Next hoặc lỗi:", e)
                
                with open(f"/app/output/{folder}/{save}/player_info/salary/player_salary.json", "w", encoding="utf-8") as f:
                    json.dump(player_salary, f, ensure_ascii=False, indent=2)


    def crawl_capology_payroll(self):
        for league, folder in league_list.items():
            for year, save in year_list.items():
                team_payroll = []
                url = f"{self.base_url}{league}payrolls/{year}"
                print(url)
                driver = self.get_driver()
                driver.get(url)
                while(True):
                    time.sleep(10)
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    time.sleep(1)
                    league_table = soup.find('table', class_="table table-bordered table-hover").find('tbody')
                    team_list = league_table.find_all('tr')
                    if not team_list:
                        print("không tìm dc")
                    for item in team_list:
                        club_and_player_name = item.find_all('a')
                        team_name = club_and_player_name[0].text.strip()
                        attribute = item.find_all('td')
                        gross_per_w = attribute[2].text.strip()
                        gross_per_y = attribute[3].text.strip()
                        keeper = attribute[5].text.strip()
                        defense = attribute[6].text.strip()
                        midfield = attribute[7].text.strip()
                        forward = attribute[8].text.strip()
                        this_data = {
                            "team":team_name,
                            "gross/w" : gross_per_w,
                            "gross/y" : gross_per_y,
                            "gross_keeper" : keeper,
                            "gross_defense": defense,
                            "gross_midfield" : midfield,
                            "gross_forward" : forward,
                        }
                        print(this_data)
                        team_payroll.append(this_data)
                    break               
                with open(f"/app/output/{folder}/{save}/team_info/team_finance/club_payrolls.json", "w", encoding="utf-8") as f:
                    json.dump(team_payroll, f, ensure_ascii=False, indent=2)
                driver.quit()

    def crawl_capology_transfer(self):
        for league, folder in league_list.items():
            for year, save in year_list.items():
                team_transfer = []
                url = f"{self.base_url}{league}transfer-window/{year}"
                print(url)
                driver = self.get_driver()
                driver.get(url)
                while(True):
                    time.sleep(8)
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    time.sleep(1)
                    league_table = soup.find('table', class_="table table-bordered table-hover").find('tbody')
                    team_list = league_table.find_all('tr')
                    if not team_list:
                        print("không tìm dc")
                    for item in team_list:
                        club_and_player_name = item.find_all('a')
                        team_name = club_and_player_name[0].text.strip()
                        attribute = item.find_all('td')
                        income = attribute[2].text.strip()
                        expense = attribute[3].text.strip()
                        balance = attribute[5].text.strip()
                        this_data = {
                            "team":team_name,
                            "incomes" : income,
                            "expense" : expense,
                            "balance" : balance,
                        }
                        print(this_data)
                        team_transfer.append(this_data)
                    break
                
                with open(f"/app/output/{folder}/{save}/team_info/team_finance/club_transfer.json", "w", encoding="utf-8") as f:
                    json.dump(team_transfer, f, ensure_ascii=False, indent=2)
                driver.quit()

base_url = "https://www.capology.com"
def save_data_all():
    Scraping(base_url).crawl_capology_salary()
    Scraping(base_url).crawl_capology_payroll()
    Scraping(base_url).crawl_capology_transfer()

save_data_all()
