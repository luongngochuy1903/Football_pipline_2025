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
    area VARCHAR,
    league_name VARCHAR,
    type VARCHAR,
    season VARCHAR
);

CREATE TABLE IF NOT EXISTS core.news (
    id_news BIGINT GENERATED ALWAYS AS IDENTITY,
    headline VARCHAR,
    published TIMESTAMP,
    categories VARCHAR,
    url VARCHAR
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
    id_club_name BIGINT GENERATED ALWAYS AS INDENTITY,
    club_name VARCHAR,
    league VARCHAR
    income DOUBLE,
    expense DOUBLE,
    balance DOUBLE,
    season VARCHAR
);

CREATE TABLE IF NOT EXISTS core.club_expense (
    id_club_name BIGINT GENERATED ALWAYS AS INDENTITY,
    club_name VARCHAR,
    league VARCHAR,
    gross_week DOUBLE,
    gross_year DOUBLE,
    keeper DOUBLE,
    defense DOUBLE,
    midfield DOUBLE,
    forward DOUBLE,
    season VARCHAR
);

CREATE TABLE IF NOT EXISTS core.player_info (
    id BIGINT GENERATED ALWAYS AS INDENTITY,
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
    season VARCHAR
);

CREATE TABLE IF NOT EXISTS core.player_attacking_fact (
    id BIGINT GENERATED ALWAYS AS INDENTITY,
    club_name VARCHAR,
    player_name VARCHAR,
    sca DOUBLE,
    sca90 DOUBLE,
    gca DOUBLE,
    gca90 DOUBLE,
    passlive INTEGER,
    season VARCHAR
);

CREATE TABLE IF NOT EXISTS core.player_defending_fact (
    id BIGINT GENERATED ALWAYS AS INDENTITY,
    club_name VARCHAR,
    player_name VARCHAR,
    tkl INTEGER,
    tklWon INTEGER,
    tkl_pct DOUBLE,
    interception INTEGER,
    blocks INTEGER,
    errors INTEGER,
    season VARCHAR
);

CREATE TABLE IF NOT EXISTS core.player_salary (
    id BIGINT GENERATED ALWAYS AS INDENTITY,
    player_name VARCHAR,
    club_name VARCHAR,
    league VARCHAR,
    gross_week DOUBLE,
    gross_year DOUBLE,
    signed TIMESTAMP,
    expiration TIMESTAMP,
    gross_remaining DOUBLE,
    release_clause DOUBLE,
    season VARCHAR
);
    -- Create mapping table
CREATE TABLE IF NOT EXISTS core.league_mapping 
(
    raw_name VARCHAR PRIMARY KEY,
    standard_name VARCHAR
);
INSERT INTO core.league_mapping VALUES(
    
)
    """
conn.execute(sql_script)