import duckdb
conn = duckdb.connect("/app/volume/datawarehouse.duckdb")
sql_script = """
-- Create schema
CREATE SCHEMA IF NOT EXISTS core;

-- Drop and create tables in staging
CREATE TABLE IF NOT EXISTS core.season (
    season VARCHAR,
    start_date TIMESTAMP,
    end_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS core.leagues (
    league_name VARCHAR,
    area VARCHAR,
    id_season INTEGER
);

CREATE TABLE IF NOT EXISTS core.news (
    published TIMESTAMP,
    headline VARCHAR,
    url VARCHAR,
    categories VARCHAR
);

CREATE TABLE IF NOT EXISTS core.club_info (
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

CREATE TABLE IF NOT EXISTS core.club_transfer (
    club_name VARCHAR,
    income DOUBLE,
    expense DOUBLE,
    balance DOUBLE,
    season VARCHAR
);

CREATE TABLE IF NOT EXISTS core.club_expense (
    club_name VARCHAR,
    "gross/week" DOUBLE,
    "gross/year" DOUBLE,
    keeper DOUBLE,
    defense DOUBLE,
    midfield DOUBLE,
    forward DOUBLE,
    season VARCHAR
);

CREATE TABLE IF NOT EXISTS core.player_info (
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

CREATE TABLE IF NOT EXISTS core.player_attacking_fact (
    player_name VARCHAR,
    club_name VARCHAR,
    sca DOUBLE,
    sca90 DOUBLE,
    gca DOUBLE,
    gca90 DOUBLE,
    passlive INTEGER,
    season VARCHAR
);

CREATE TABLE IF NOT EXISTS core.player_defending_fact (
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

CREATE TABLE IF NOT EXISTS core.player_salary (
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