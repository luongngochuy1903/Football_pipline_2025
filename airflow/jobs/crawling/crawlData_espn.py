
from bs4 import BeautifulSoup
import time
import json
import requests
import datetime
list_news = ["http://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/news",
             "http://site.api.espn.com/apis/site/v2/sports/soccer/esp.1/news",
             "http://site.api.espn.com/apis/site/v2/sports/soccer/ita.1/news",
             "http://site.api.espn.com/apis/site/v2/sports/soccer/fra.1/news",
             "http://site.api.espn.com/apis/site/v2/sports/soccer/ger.1/news"]

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

def crawl_league_news():
    news_map = {
                    "eng.1": "/opt/shared/news/espn/eng_news.json",
                    "esp.1": "/opt/shared/news/espn/esp_news.json",
                    "fra.1": "/opt/shared/news/espn/fra_news.json",
                    "ger.1": "/opt/shared/news/espn/ger_news.json",
                    "ita.1": "/opt/shared/news/espn/ita_news.json"
                }
    for item in list_news:
        response = requests.get(item)
        time.sleep(2)
        if response.status_code == 200:
            data = [response.json()]
            # newdata = []
            # for article in data["articles"]:
            #     partdata = {
            #         "Headline": article["headline"],
            #         "Published": article["published"],
            #         "Url":"",
            #             }
            #     list_tag = [
            #         tag.get("description") for tag in article.get("categories", [])
            #         if tag.get("description")
            #     ]
            #     partdata["Tags"] = list_tag
            #     newdata.append(partdata)
            for code, target_list in news_map.items():
                if code in item:
                    with open(target_list, "w", encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    # print(json.dumps(target_list, indent=2, ensure_ascii=False))
                    break
        else:
            print("fail")

def crawl_team_season():
    league_map = {
                    "PL": "/opt/shared/premier_leagues/24_25/team_info/team_season.json",
                    "PD": "/opt/shared/premier_leagues/24_25/team_info/team_season.json",
                    "FL1": "/opt/shared/premier_leagues/24_25/team_info/team_season.json",
                    "BL1": "/opt/shared/premier_leagues/24_25/team_info/team_season.json",
                    "SA": "/opt/shared/premier_leagues/24_25/team_info/team_season.json"
                }
    for item in list_url:
        for year, year_code in list_year.items():
            time.sleep(3)
            response = requests.get(f"{item}{year}", headers=headers)
            if response.status_code == 200:
                data = [response.json()]
                # newdata = [
                #     {"Season":data["filters"]["season"],
                #      "Area":data["area"]["name"],
                #      "League":data["competition"]["name"],
                #      "Start_date": data["season"]["startDate"],
                #      "End_date": data["season"]["endDate"]
                #     }
                # ] # giữ lại khi vào refined zone
                for code, target_list in league_map.items():
                    if code in item:
                        with open(target_list, "w") as f:
                            json.dump(data, f, indent=2)
                        break
            else:
                print("fail")
    
# crawl_league_news()
# crawl_team_season()