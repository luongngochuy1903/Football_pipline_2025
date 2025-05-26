
from bs4 import BeautifulSoup
import time
import json
import requests
import datetime

list_url = ["https://api.football-data.org/v4/competitions/PL/standings/?season=", 
            "https://api.football-data.org/v4/competitions/PD/standings/?season=",
            "https://api.football-data.org/v4/competitions/FL1/standings/?season=",
            "https://api.football-data.org/v4/competitions/BL1/standings/?season=",
            "https://api.football-data.org/v4/competitions/SA/standings/?season="]

headers = {
    "X-Auth-Token":"709bd6b18c67431d815ceb0bb15e0690"
}
list_year = {"2024":"24_25", 
              }

def crawl_team_season():
    league_map = {
                    "PL": "/opt/shared/premier_leagues/24_25/team_info/team_season.json",
                    "PD": "/opt/shared/laliga/24_25/team_info/team_season.json",
                    "FL1": "/opt/shared/ligue 1/24_25/team_info/team_season.json",
                    "BL1": "/opt/shared/bundesliga/24_25/team_info/team_season.json",
                    "SA": "/opt/shared/seria/24_25/team_info/team_season.json"
                }
    for item in list_url:
        for year, year_code in list_year.items():
            time.sleep(3)
            response = requests.get(f"{item}{year}", headers=headers)
            if response.status_code == 200:
                data = response.json()
                for code, target_list in league_map.items():
                    if code in item:
                        with open(target_list, "w") as f:
                            json.dump(data, f, indent=2)
                        break
            else:
                print("fail")
    
crawl_team_season()