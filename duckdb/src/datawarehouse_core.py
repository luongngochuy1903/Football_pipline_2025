import duckdb
conn = duckdb.connect("/app/volume/datawarehouse.duckdb")
"""
    Setting up core layer data warehouse
"""
sql_script = """
-- Create schema
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.season (
    season VARCHAR,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
);
CREATE SEQUENCE id_league_sequence START 1;
CREATE TABLE IF NOT EXISTS core.leagues (
    id_league INTEGER DEFAULT nextval('id_league_sequence'),
    area VARCHAR,
    league_name VARCHAR,
    type VARCHAR,
    season VARCHAR,
);

CREATE SEQUENCE id_news_sequence START 1;
CREATE TABLE IF NOT EXISTS core.news (
    id_news INTEGER DEFAULT nextval('id_news_sequence'),
    headline VARCHAR,
    published TIMESTAMP,
    categories VARCHAR,
    url VARCHAR 
);

CREATE SEQUENCE id_club_info START 1;
CREATE TABLE IF NOT EXISTS core.club_info (
    id_club_name INTEGER DEFAULT nextval('id_club_info'),
    club_name VARCHAR,
    league VARCHAR,
    point INTEGER,
    position INTEGER,
    manager VARCHAR,
    playedGames INTEGER,
    won INTEGER,
    draw INTEGER,
    lost INTEGER,
    goalDifference INTEGER,
    goalsFor INTEGER,
    goalsAgainst INTEGER,
    season VARCHAR,
    valid_to DATE DEFAULT DATE '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE 
);

CREATE SEQUENCE id_club_transfer START 1;
CREATE TABLE IF NOT EXISTS core.club_transfer (
    id_club_name INTEGER DEFAULT nextval('id_club_transfer'),
    club_name VARCHAR,
    league VARCHAR,
    income DOUBLE,
    expense DOUBLE,
    balance DOUBLE,
    season VARCHAR,
    valid_to DATE DEFAULT DATE '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE 
);

CREATE SEQUENCE id_club_expense START 1;
CREATE TABLE IF NOT EXISTS core.club_expense (
    id_club_name INTEGER DEFAULT nextval('id_club_expense'),
    club_name VARCHAR,
    league VARCHAR,
    gross_week DOUBLE,
    gross_year DOUBLE,
    keeper DOUBLE,
    defense DOUBLE,
    midfield DOUBLE,
    forward DOUBLE,
    season VARCHAR,
    valid_to DATE DEFAULT DATE '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE 
);

CREATE SEQUENCE id_player_info START 1;
CREATE TABLE IF NOT EXISTS core.player_info (
    id INTEGER DEFAULT nextval('id_player_info'),
    club_name VARCHAR,
    player_name VARCHAR,
    match_played INTEGER,
    age INTEGER,
    nationality VARCHAR,
    minutes INTEGER,
    position VARCHAR,
    goal INTEGER,
    assist INTEGER,
    xG DOUBLE,
    xAG DOUBLE,
    season VARCHAR,
    valid_to DATE DEFAULT DATE '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE 
);

CREATE SEQUENCE id_attacking_fact START 1;
CREATE TABLE IF NOT EXISTS core.player_attacking_fact (
    id INTEGER DEFAULT nextval('id_attacking_fact'),
    club_name VARCHAR,
    player_name VARCHAR,
    sca DOUBLE,
    sca90 DOUBLE,
    gca DOUBLE,
    gca90 DOUBLE,
    passlive INTEGER,
    season VARCHAR,
    valid_to DATE DEFAULT DATE '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE 
);

CREATE SEQUENCE id_defending_fact START 1;
CREATE TABLE IF NOT EXISTS core.player_defending_fact (
    id INTEGER DEFAULT nextval('id_defending_fact'),
    club_name VARCHAR,
    player_name VARCHAR,
    tkl INTEGER,
    tklWon INTEGER,
    tkl_pct DOUBLE,
    interception INTEGER,
    blocks INTEGER,
    errors INTEGER,
    season VARCHAR,
    valid_to DATE DEFAULT DATE '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE 
);

CREATE SEQUENCE id_player_salary START 1;
CREATE TABLE IF NOT EXISTS core.player_salary (
    id INTEGER DEFAULT nextval('id_player_salary'),
    player_name VARCHAR,
    club_name VARCHAR,
    league VARCHAR,
    gross_week DOUBLE,
    gross_year DOUBLE,
    signed TIMESTAMP,
    expiration TIMESTAMP,
    gross_remaining DOUBLE,
    release_clause DOUBLE,
    season VARCHAR,
    valid_to DATE DEFAULT DATE '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE 
);

    """
conn.execute(sql_script)