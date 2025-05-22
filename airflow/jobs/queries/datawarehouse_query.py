#All queries of building up table in data warehouse
class AnalyticsQuery:
    create_schema = """
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS core;
    CREATE SCHEMA IF NOT EXISTS mart;
"""
    create_season = """
    CREATE TABLE IF NOT EXISTS core.season
    (
        id_season INTEGER PRIMARY KEY,
        season VARCHAR(10),
        start_date DATETIME,
        end_date DATETIME
    );
"""
    create_league = """
    CREATE TABLE IF NOT EXISTS core.leagues
    (
        id_leagues INTEGER PRIMARY KEY,
        league_name VARCHAR(100),
        area VARCHAR(100),
        id_news INTEGER
    );
"""
    create_news = """
    CREATE TABLE IF NOT EXISTS core.news
    (
        id_news INTEGER PRIMARY KEY,
        published DATETIME,
        headline VARCHAR(400),
        tag VARCHAR(400)
    );
"""
    create_club_info ="""
    CREATE TABLE IF NOT EXISTS core.club_info
    (
        id_club INTEGER PRIMARY KEY,
        id_season INTEGER,
        id_leagues INTEGER,
        club_name VARCHAR(100),
        point INTEGER,
        position INTEGER,
        manager VARCHAR(255)
    );
"""
    create_club_transfer ="""
    CREATE TABLE IF NOT EXISTS core.club_transfer
    (
        id_club INTEGER PRIMARY KEY,
        id_season INTEGER,
        income FLOAT,
        expense FLOAT,
        balance FLOAT
    );
"""
    create_club_expense ="""
    CREATE TABLE IF NOT EXISTS core.club_expense
    (
        id_club INTEGER PRIMARY KEY,
        id_season INTEGER,
        gross/week FLOAT,
        gross/year FLOAT,
        keeper FLOAT,
        defense FLOAT,
        midfield FLOAT,
        forward FLOAT
    );
"""
    create_player_info ="""
    CREATE TABLE IF NOT EXISTS core.player_info
    (
        id_player INTEGER PRIMARY KEY,
        id_club INTEGER,
        player_name VARCHAR(255),
        nationality VARCHAR(100),
        position VARCHAR(4),
        age INTEGER
    );
"""
    create_player_attacking_fact ="""
    CREATE TABLE IF NOT EXISTS core.player_attacking_fact
    (
        id INTEGER PRIMARY KEY,
        id_player INTEGER,
        id_season INTEGER,
        sca FLOAT,
        sca90 FLOAT,
        gca FLOAT,
        gca90 FLOAT,
        goal INTEGER,
        assist INTEGER,
        xg FLOAT,
        xAg FLOAT,
        Passlive INTEGER
    );
"""
    create_player_defending_fact ="""
    CREATE TABLE IF NOT EXISTS core.player_defending_fact
    (
        id INTEGER PRIMARY KEY,
        id_player INTEGER,
        id_season INTEGER,
        tkl INTEGER,
        tklWon INTEGER,
        tkl_pct FLOAT,
        interception INTEGER,
        blocks INTEGER,
        error INTEGER
    );
"""
    create_player_salary ="""
    CREATE TABLE IF NOT EXISTS core.player_salary
    (
        id INTEGER PRIMARY KEY,
        id_player INTEGER,
        id_season INTEGER,
        gross_week FLOAT,
        gross_year FLOAT,
        signed DATETIME,
        expiration DATETIME,
        gross_remaining FLOAT,
        release_clause FLOAT
    );
"""