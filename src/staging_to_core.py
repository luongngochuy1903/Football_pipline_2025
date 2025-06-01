import duckdb
conn = duckdb.connect("/app/volume/datawarehouse.duckdb")
sql_script = """
    -- Transforming staging.season
    INSERT INTO core.season
    SELECT * FROM staging.season;
    
    -- Transforming staging.leagues
    INSERT INTO core.leagues
    SELECT * FROM staging.leagues
    WHERE EXISTS (SELECT 1 FROM staging.leagues);

    -- Transforming staging.news
    INSERT INTO core.news
    SELECT * FROM staging.leagues;

    -- Transforming staging.club_info
    INSERT INTO core.club_info
    SELECT * FROM staging.club_info;;

    -- Transforming staging.club_transfer
    INSERT INTO core.club_transfer
    SELECT * FROM staging.club_transfer;

    -- Transforming staging.club_expense
    INSERT INTO core.club_expense
    SELECT * FROM staging.club_expense;

    -- Transforming staging.player_info
    INSERT INTO core.player_info
    SELECT * FROM staging.player_info;

    -- Transforming staging.player_attacking_fact
    INSERT INTO core.player_attacking_fact
    SELECT * FROM staging.player_attacking_fact;

    -- Transforming staging.player_defending_fact
    INSERT INTO core.player_defending_fact
    SELECT * FROM staging.player_defending_fact;

    -- Transforming staging.player_salary
    INSERT INTO core.player_salary
    SELECT * FROM staging.player_salary;
"""