-- Create schema
CREATE SCHEMA IF NOT EXISTS staging;

-- Drop and create tables in staging
DROP TABLE IF EXISTS staging.season;
CREATE TABLE staging.season (
    id_season INTEGER,
    season VARCHAR,
    start_date TIMESTAMP,
    end_date TIMESTAMP
);

DROP TABLE IF EXISTS staging.leagues;
CREATE TABLE staging.leagues (
    id_leagues INTEGER,
    league_name VARCHAR,
    area VARCHAR,
    id_news INTEGER
);

DROP TABLE IF EXISTS staging.news;
CREATE TABLE staging.news (
    id_news INTEGER,
    published TIMESTAMP,
    headline VARCHAR,
    tag VARCHAR
);

DROP TABLE IF EXISTS staging.club_info;
CREATE TABLE staging.club_info (
    id_club INTEGER,
    id_season INTEGER,
    id_leagues INTEGER,
    club_name VARCHAR,
    point INTEGER,
    position INTEGER,
    manager VARCHAR
);

DROP TABLE IF EXISTS staging.club_transfer;
CREATE TABLE staging.club_transfer (
    id_club INTEGER,
    id_season INTEGER,
    income DOUBLE,
    expense DOUBLE,
    balance DOUBLE
);

DROP TABLE IF EXISTS staging.club_expense;
CREATE TABLE staging.club_expense (
    id_club INTEGER,
    id_season INTEGER,
    "gross/week" DOUBLE,
    "gross/year" DOUBLE,
    keeper DOUBLE,
    defense DOUBLE,
    midfield DOUBLE,
    forward DOUBLE
);

DROP TABLE IF EXISTS staging.player_info;
CREATE TABLE staging.player_info (
    id_player INTEGER,
    id_club INTEGER,
    player_name VARCHAR,
    nationality VARCHAR,
    position VARCHAR,
    age INTEGER
);

DROP TABLE IF EXISTS staging.player_attacking_fact;
CREATE TABLE staging.player_attacking_fact (
    id INTEGER,
    id_player INTEGER,
    id_season INTEGER,
    sca DOUBLE,
    sca90 DOUBLE,
    gca DOUBLE,
    gca90 DOUBLE,
    goal INTEGER,
    assist INTEGER,
    xg DOUBLE,
    xAg DOUBLE,
    Passlive INTEGER
);

DROP TABLE IF EXISTS staging.player_defending_fact;
CREATE TABLE staging.player_defending_fact (
    id INTEGER,
    id_player INTEGER,
    id_season INTEGER,
    tkl INTEGER,
    tklWon INTEGER,
    tkl_pct DOUBLE,
    interception INTEGER,
    blocks INTEGER,
    error INTEGER
);

DROP TABLE IF EXISTS staging.player_salary;
CREATE TABLE staging.player_salary (
    id INTEGER,
    id_player INTEGER,
    id_season INTEGER,
    gross_week DOUBLE,
    gross_year DOUBLE,
    signed TIMESTAMP,
    expiration TIMESTAMP,
    gross_remaining DOUBLE,
    release_clause DOUBLE
);
