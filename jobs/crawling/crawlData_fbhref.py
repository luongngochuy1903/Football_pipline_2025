# import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import json
import random
import os
import stat

year_to_scrap = ["2024-2025"]
league_to_scrap = {"Ligue-1-Stats":"13","Premier-League-Stats":"9", "La-Liga-Stats":"12","Bundesliga-Stats":"20","Serie-A-Stats":"11"
                   }
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

    def get_team_url(self):
        team_map = {
                    "Premier-League-Stats": "premier_leagues/24_25/team_info/teams_overall.json",
                    "Bundesliga-Stats": "bundesliga/24_25/team_info/teams_overall.json",
                    "Ligue-1-Stats": "ligue 1/24_25/team_info/teams_overall.json",
                    "Serie-A-Stats": "seria/24_25/team_info/teams_overall.json",
                    "La-Liga-Stats": "laliga/24_25/team_info/teams_overall.json"
                }
        base_url = "https://fbref.com/"

        driver = self.get_driver()

        for leagues,id in league_to_scrap.items():
            for item in year_to_scrap:
                now_url = f"https://fbref.com/en/comps/{id}/{item}/{item}-{leagues}"
                print(now_url)
                driver.get(now_url)
                time.sleep(6)
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                time.sleep(2)
                print(f"div_results{item}91_overall")
                table = soup.find('div', id=f"div_results{item}{id}1_overall")
                if not table:
                    print("Không tìm thấy table")
                    continue

                tbody = table.find('tbody')
                if not tbody:
                    print("Không tìm thấy tbody")
                    continue

                get_table = tbody.find_all('tr')
                club_info_list = []
                for element in get_table:
                    rank = element.find('th').text.strip()
                    atribute = element.find_all('td')
                    team_name, point, manager_name = "", "", ""

                    for items in atribute:
                        if items.get('data-stat', '') == "team":
                            team_detail_url = base_url + items.find('a')['href']
                            team_name = items.find('a').text.strip()
                            print(f"Team Detail URL: {team_detail_url}")
                            time.sleep(1)
                            driver.get(team_detail_url)
                            try:
                                WebDriverWait(driver, 15).until(
                                    EC.presence_of_element_located((By.TAG_NAME, "div"))
                                )
                            except Exception as e:
                                print(f"Lỗi khi tải trang chi tiết đội: {e}")
                                continue
                            
                            soup2 = BeautifulSoup(driver.page_source, 'html.parser')
                            team_summary = soup2.find('div', {'data-template': 'Partials/Teams/Summary'})
                            if team_summary:
                                manager_tag = team_summary.find('strong', string='Manager:')
                                manager_name = manager_tag.next_sibling.strip() if manager_tag else "Unknown"

                        if items.get('data-stat', '') == "points":
                            point = items.text.strip()

                    this_data = {
                        "team_name": team_name,
                        "rank": rank,
                        "point": point,
                        "manager": manager_name,
                        "season": item
                    }
                    club_info_list.append(this_data)
                print(club_info_list)
                for code, address in team_map.items():
                    if code == leagues:
                        with open(f"/app/output/{address}","w",encoding='utf-8') as f:
                            json.dump(club_info_list,f,indent=2, ensure_ascii=False) 
                        print(f"đã lưu vào {address}")
                        break
            time.sleep(2)  
        driver.quit() 

            
    def get_player_info(self):
        team_map = {
                    "Premier-League-Stats": "premier_leagues/24_25/player_info",
                    "Bundesliga-Stats": "bundesliga/24_25/player_info",
                    "Ligue-1-Stats": "ligue 1/24_25/player_info",
                    "Serie-A-Stats": "seria/24_25/player_info",
                    "La-Liga-Stats": "laliga/24_25/player_info"
                }
        base_url = "https://fbref.com/"

        driver = self.get_driver()

        for leagues, id in league_to_scrap.items():
            for item in year_to_scrap:
                now_url = f"https://fbref.com/en/comps/{id}/{item}/{item}-{leagues}"
                print(now_url)
                driver.get(now_url)
                time.sleep(5)
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                time.sleep(2)
                print(f"div_results{item}91_overall")
                table = soup.find('div', id=f"div_results{item}{id}1_overall") 
                if not table:
                    print("Không tìm thấy table")
                    continue

                tbody = table.find('tbody')
                if not tbody:
                    print("Không tìm thấy tbody")
                    continue

                get_table = tbody.find_all('tr')
                
                temp_team_attack = [] 
                temp_team_info = []
                temp_team_defend = []
                for element in get_table:
                    atribute = element.find('td')
                    team_name = ""
                    team_detail_url = base_url + atribute.find('a')['href']
                    team_name = atribute.find('a').text.strip()
                    print(f"Team Detail URL: {team_detail_url}")

                    driver.get(team_detail_url)
                    try:
                        WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.TAG_NAME, "table"))
                        )
                    except Exception as e:
                        print(f"Lỗi khi tải trang chi tiết đội: {e}")
                        continue
                    time.sleep(4)
                    soup2 = BeautifulSoup(driver.page_source, 'html.parser')
                    player_info = soup2.find('table', class_="stats_table sortable min_width now_sortable sticky_table eq1 re1 le1")
                    player_info2 = player_info.find('tbody')
                    player_info3 = player_info2.find_all('tr')
                    if player_info3:
                        print("có player_info")
                    for players in player_info3:
                        nationality = ""
                        position = ""
                        age = ""
                        minutes = ""
                        match_played = ""
                        assist = ""
                        goal = ""
                        xG = ""
                        xAG = ""
                        player_atribute = players.find_all('td')
                        player_name = players.find('th').text.strip()
                        for attr in player_atribute:
                            if "nationality" in attr.get('data-stat',''):
                                nationality = attr.text.strip()
                            if "position" in attr.get('data-stat',''):
                                position = attr.text.strip()
                            if attr.get('data-stat','') == "age":
                                age = attr.text.strip()
                            if attr.get('data-stat','') == "minutes":
                                minutes = attr.text.strip()
                            if attr.get('data-stat','') == "games":
                                match_played = attr.text.strip()
                            if attr.get('data-stat','') == "assists":
                                assist = attr.text.strip()
                            if attr.get('data-stat','') == "xg": 
                                xG = attr.text.strip()
                            if attr.get('data-stat','') == "goals":
                                goal = attr.text.strip()
                            if attr.get('data-stat','') == "xg_assist":
                                xAG = attr.text.strip()
                        player_fact = {
                            "team_name": team_name,
                            "player_name": player_name,
                            "match_played": match_played,
                            "nationality": nationality,
                            "minutes": minutes,
                            "position":position,
                            "age":age,
                            "goal":goal,
                            "assist":assist,
                            "xG":xG,
                            "xAG":xAG,
                            "season": item
                        }
                        print(player_fact)
                        temp_team_info.append(player_fact)
                    player_attacking = soup2.find('table', {'id':f'stats_gca_{id}'}).find('tbody').find_all('tr')
                    time.sleep(2)
                    for players in player_attacking:
                        SCA = ""
                        SCA90 = ""
                        GCA = ""
                        GCA90 = ""
                        passlive = ""
                        player_attack_atribute = players.find_all('td')
                        player_name = players.find('th').text.strip()
                        for attr in player_attack_atribute:
                            if attr.get('data-stat','') == "sca":
                                SCA = attr.text.strip()
                            if attr.get('data-stat','') == "sca_per90":
                                SCA90 = attr.text.strip()
                            if attr.get('data-stat','') == "gca":
                                GCA = attr.text.strip()
                            if attr.get('data-stat','') == "gca_per90":
                                GCA90 = attr.text.strip()
                            if attr.get('data-stat','') == "gca_passes_live":
                                passlive = attr.text.strip()
                        player_attack = {
                            "team_name": team_name,
                            "player_name": player_name,
                            "SCA":SCA,
                            "SCA90":SCA90,
                            "GCA":GCA,
                            "GCA90":GCA90,
                            "passlive":passlive,
                            "season": item
                        }   
                        temp_team_attack.append(player_attack)
                    time.sleep(2)
                    player_defending = soup2.find('table', {'id':f'stats_defense_{id}'}).find('tbody').find_all('tr')
                    for players in player_defending:
                        tkl = ""
                        tklWon =""
                        tkl_pct=""
                        interception=""
                        blocks=""
                        errors=""
                        player_defend_atribute = players.find_all('td')
                        player_name = players.find('th').text.strip()
                        for attr in player_defend_atribute:
                            if attr.get('data-stat','') == "tackles":
                                tkl = attr.text.strip()
                            if attr.get('data-stat','') == "tackles_won":
                                tklWon = attr.text.strip()
                            if attr.get('data-stat','') == "challenge_tackles_pct":
                                tkl_pct = attr.text.strip()
                            if attr.get('data-stat','') == "interceptions":
                                interception = attr.text.strip()
                            if attr.get('data-stat','') == "blocks":
                                blocks = attr.text.strip()
                            if attr.get('data-stat','') == "errors":
                                errors = attr.text.strip()
                        player_defend = {
                            "team_name": team_name,
                            "player_name": player_name,
                            "tkl":tkl,
                            "tklWon":tklWon,
                            "tkl_pct":tkl_pct,
                            "interception":interception,
                            "blocks":blocks,
                            "errors":errors,
                            "season": item
                        }   
                        temp_team_defend.append(player_defend)
                    print(temp_team_defend)
                for code, address in team_map.items():
                    if code == leagues:
                        with open(f"/app/output/{address}/overall/player_stat.json","w", encoding='utf-8') as f:
                            json.dump(temp_team_info,f,indent=2, ensure_ascii=False)
                        with open(f"/app/output/{address}/attacking/player_stat.json","w",encoding='utf-8') as f:
                            json.dump(temp_team_attack,f,indent=2, ensure_ascii=False)
                        with open(f"/app/output/{address}/defending/player_stat.json","w",encoding='utf-8') as f:
                            json.dump(temp_team_defend,f,indent=2, ensure_ascii=False)
        

base_url = "https://fbref.com/en/comps/"
def save_data_all():
    Scraping(base_url).get_team_url()
    Scraping(base_url).get_player_info()

save_data_all()
