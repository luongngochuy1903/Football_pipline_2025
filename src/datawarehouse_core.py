import duckdb
conn = duckdb.connect("/app/volume/datawarehouse.duckdb")
sql_script = """
-- Create schema
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.season (
    season VARCHAR,
    start_date TIMESTAMP,
    end_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS core.leagues (
    id_league BIGINT GENERATED ALWAYS AS IDENTITY,
    league_name VARCHAR,
    area VARCHAR,
    season VARCHAR
);

CREATE TABLE IF NOT EXISTS core.news (
    id_news BIGINT GENERATED ALWAYS AS IDENTITY,
    published TIMESTAMP,
    headline VARCHAR,
    url VARCHAR,
    categories VARCHAR
);

CREATE TABLE IF NOT EXISTS core.club_info (
    id_club_name BIGINT GENERATED ALWAYS AS INDENTITY,
    club_name VARCHAR,
    league_name VARCHAR,
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
    id_club_name INTEGER,
    club_name VARCHAR,
    income DOUBLE,
    expense DOUBLE,
    balance DOUBLE
);

CREATE TABLE IF NOT EXISTS core.club_expense (
    id_club_name INTEGER,
    id_league INTEGER,
    id_season INTEGER,
    club_name VARCHAR,
    "gross/week" DOUBLE,
    "gross/year" DOUBLE,
    keeper DOUBLE,
    defense DOUBLE,
    midfield DOUBLE,
    forward DOUBLE
);

CREATE TABLE IF NOT EXISTS core.player_info (
    id_club_name INTEGER,
    id_season INTEGER,
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
    xAG DOUBLE
);

CREATE TABLE IF NOT EXISTS core.player_attacking_fact (
    id_club_name INTEGER,
    id_season INTEGER,
    player_name VARCHAR,
    club_name VARCHAR,
    sca DOUBLE,
    sca90 DOUBLE,
    gca DOUBLE,
    gca90 DOUBLE,
    passlive INTEGER
);

CREATE TABLE IF NOT EXISTS core.player_defending_fact (
    id_club_name INTEGER,
    id_season INTEGER,
    player_name VARCHAR,
    club_name VARCHAR,
    tkl INTEGER,
    tklWon INTEGER,
    tkl_pct DOUBLE,
    interception INTEGER,
    blocks INTEGER,
    error INTEGER
);

CREATE TABLE IF NOT EXISTS core.player_salary (
    id_club_name INTEGER,
    id_season INTEGER,
    player_name VARCHAR,
    club_name VARCHAR,
    gross_week DOUBLE,
    gross_year DOUBLE,
    signed TIMESTAMP,
    expiration TIMESTAMP,
    gross_remaining DOUBLE,
    release_clause DOUBLE
);
    -- Create mapping table
CREATE TABLE IF NOT EXISTS core.league_mapping 
(
    raw_name VARCHAR PRIMARY KEY,
    standard_name VARCHAR
);
INSERT INTO core.league_mapping VALUES()
    """
conn.execute(sql_script)