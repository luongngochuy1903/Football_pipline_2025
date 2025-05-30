import duckdb
conn = duckdb.connect("/app/volume/datawarehouse.duckdb")

sql_script = """
-- Create schema
CREATE SCHEMA IF NOT EXISTS staging;

-- Drop and create tables in staging
DROP TABLE IF EXISTS staging.season;
CREATE TABLE staging.season (
    season VARCHAR,
    start_date TIMESTAMP,
    end_date TIMESTAMP
);

DROP TABLE IF EXISTS staging.leagues;
CREATE TABLE staging.leagues (
    league_name VARCHAR,
    area VARCHAR,
    season VARCHAR
);

DROP TABLE IF EXISTS staging.news;
CREATE TABLE staging.news (
    published TIMESTAMP,
    headline VARCHAR,
    url VARCHAR,
    categories VARCHAR
);

DROP TABLE IF EXISTS staging.club_info;
CREATE TABLE staging.club_info (
    club_name VARCHAR,
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
    season VARCHAR
);

DROP TABLE IF EXISTS staging.club_transfer;
CREATE TABLE staging.club_transfer (
    club_name VARCHAR,
    income DOUBLE,
    expense DOUBLE,
    balance DOUBLE,
    season VARCHAR
);

DROP TABLE IF EXISTS staging.club_expense;
CREATE TABLE staging.club_expense (
    club_name VARCHAR,
    gross_week DOUBLE,
    gross_year DOUBLE,
    keeper DOUBLE,
    defense DOUBLE,
    midfield DOUBLE,
    forward DOUBLE,
    season VARCHAR
);

DROP TABLE IF EXISTS staging.player_info;
CREATE TABLE staging.player_info (
    player_name VARCHAR,
    club_name VARCHAR,
    match_played INTEGER,
    nationality VARCHAR,
    minutes INTEGER,
    position VARCHAR,
    age INTEGER,
    goal INTEGER,
    assist INTEGER,
    xG DOUBLE,
    xAG DOUBLE,
    season VARCHAR
);

DROP TABLE IF EXISTS staging.player_attacking_fact;
CREATE TABLE staging.player_attacking_fact (
    player_name VARCHAR,
    club_name VARCHAR,
    sca DOUBLE,
    sca90 DOUBLE,
    gca DOUBLE,
    gca90 DOUBLE,
    passlive INTEGER,
    season VARCHAR
);

DROP TABLE IF EXISTS staging.player_defending_fact;
CREATE TABLE staging.player_defending_fact (
    player_name VARCHAR,
    club_name VARCHAR,
    tkl INTEGER,
    tklWon INTEGER,
    tkl_pct DOUBLE,
    interception INTEGER,
    blocks INTEGER,
    error INTEGER,
    season VARCHAR
);

DROP TABLE IF EXISTS staging.player_salary;
CREATE TABLE staging.player_salary (
    player_name VARCHAR,
    club_name VARCHAR,
    gross_week DOUBLE,
    gross_year DOUBLE,
    signed TIMESTAMP,
    expiration TIMESTAMP,
    gross_remaining DOUBLE,
    release_clause DOUBLE,
    season VARCHAR
);
    """

conn.execute(sql_script)