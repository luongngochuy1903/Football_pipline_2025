import duckdb

conn = duckdb.connect("/app/volume/datawarehouse.duckdb")
#LOADING TO NEWS
season_count = conn.execute("SELECT COUNT(*) FROM staging.season").fetchone()[0]
if season_count > 0:
    conn.execute("""
        INSERT INTO core.season
        SELECT 
            season, start_date, end_date
        FROM staging.season;
    """)

#LOADING TO LEAGUES
league_count = conn.execute("SELECT COUNT(*) FROM staging.leagues").fetchone()[0]
if league_count > 0:
    conn.execute("""
        INSERT INTO core.leagues
        SELECT 
            area, league_name, type, season
        FROM staging.leagues;
    """)
#LOADING TO NEWS
news_count = conn.execute("SELECT COUNT(*) FROM staging.news").fetchone()[0]
if news_count > 0:
    conn.execute("""
        INSERT INTO core.news
        SELECT 
            headline, published, categories, url
        FROM staging.news
                 """)

#LOADING TO CLUB_INFO
club_info_count = conn.execute("SELECT COUNT(*) FROM staging.club_info").fetchone()[0]
if club_info_count > 0:
    conn.execute("""
        UPDATE core.club_info c
        SET valid_to = make_date(s.year, s.month, s.day), is_current = FALSE
            FROM (
                 SELECT * FROM staging.club_info
                 ) s
            WHERE c.playedGames != s.playedGames
                 AND c.club_name = s.club_name
                 AND c.season = s.season
                 AND c.is_current = TRUE
                 """)
    conn.execute("""
        INSERT INTO core.club_info(club_name, league, point, position, manager, playedGames, won, 
                 draw, lost, goalDifference, goalsFor, goalsAgainst, season)
        SELECT 
            s.club_name, s.league, s.point, s.position, s.manager, s.playedGames, s.won, 
                 s.draw, s.lost, s.goalDifference, s.goalsFor, s.goalsAgainst, s.season
        FROM staging.club_info s
        LEFT JOIN core.club_info c
            ON s.club_name = c.club_name AND s.season = c.season
        WHERE (
            c.club_name IS NULL OR
            c.point IS DISTINCT FROM s.point OR
            c.position IS DISTINCT FROM s.position OR
            c.manager IS DISTINCT FROM s.manager OR
            c.playedGames IS DISTINCT FROM s.playedGames OR
            c.won IS DISTINCT FROM s.won OR
            c.draw IS DISTINCT FROM s.draw OR
            c.lost IS DISTINCT FROM s.lost OR
            c.goalDifference IS DISTINCT FROM s.goalDifference OR
            c.goalsFor IS DISTINCT FROM s.goalsFor OR
            c.goalsAgainst IS DISTINCT FROM s.goalsAgainst
            )
                 """)

#LOADNG TO CLUB_TRANSFER
club_transfer_count = conn.execute("SELECT COUNT(*) FROM staging.club_transfer").fetchone()[0]
if club_transfer_count > 0:
    conn.execute("""
        UPDATE core.club_transfer c
        SET valid_to = make_date(s.year, s.month, s.day), is_current = FALSE
            FROM (
                 SELECT * FROM staging.club_transfer
                 ) s
            WHERE c.club_name = s.club_name
                 AND c.is_current = TRUE 
                 AND (
                    c.income != s.income OR
                    c.expense != s.expense OR
                    c.balance != s.balance
                 )
                 """)
    conn.execute("""
        INSERT INTO core.club_transfer(club_name, league, income, expense, balance, season)
        SELECT 
            s.club_name, s.league, s.income, s.expense, s.balance, s.season
        FROM staging.club_transfer s
        LEFT JOIN core.club_transfer c
            ON s.club_name = c.club_name AND s.season = c.season
        WHERE (
            c.club_name IS NULL OR
            c.income IS DISTINCT FROM s.income OR
            c.expense IS DISTINCT FROM s.expense OR
            c.balance IS DISTINCT FROM s.balance OR
            )
                 """)

#LOADNG TO CLUB_EXPENSE
club_expense_count = conn.execute("SELECT COUNT(*) FROM staging.club_expense").fetchone()[0]
if club_expense_count > 0:
    conn.execute("""
        UPDATE core.club_expense c
        SET valid_to = make_date(s.year, s.month, s.day), is_current = FALSE
            FROM (
                 SELECT * FROM staging.club_expense
                 ) s
            WHERE c.club_name = s.club_name
                 AND c.is_current = TRUE 
                 AND (
                    c.gross_week != s.gross_week OR
                    c.gross_year != s.gross_year OR
                    c.keeper != s.keeper OR
                    c.defense != s.defense OR
                    c.midfield != s.midfield OR
                    c.forward != s.forward OR
                 )
                 """)
    conn.execute("""
        INSERT INTO core.club_expense(club_name, league, gross_week, gross_year, keeper, defense, midfield, forward, season)
        SELECT 
            s.club_name, s.league, s.gross_week, s.gross_year, s.keeper, s.defense, s.midfield, s.forward, s.season
        FROM staging.club_expense s
        LEFT JOIN core.club_expense c
            ON s.club_name = c.club_name AND s.season = c.season
        WHERE (
            c.club_name IS NULL OR
            c.gross_week IS DISTINCT FROM s.gross_week OR
            c.gross_year IS DISTINCT FROM s.gross_year OR
            c.keeper IS DISTINCT FROM s.keeper OR
            c.defense IS DISTINCT FROM s.defense OR
            c.midfield IS DISTINCT FROM s.midfield OR
            c.forward IS DISTINCT FROM s.forward OR
            )
                 """)

#LOADING TO player_info
player_info_count = conn.execute("SELECT COUNT(*) FROM staging.player_info").fetchone()[0]
if player_info_count > 0:
    conn.execute("""
        UPDATE core.player_info c
        SET valid_to = make_date(s.year, s.month, s.day), is_current = FALSE
            FROM (
                 SELECT * FROM staging.player_info
                 ) s
            WHERE c.club_name = s.club_name
                 AND c.player_name = s.player_name
                 AND c.is_current = TRUE 
                 AND (
                    c.match_played != s.match_played OR
                    c.minutes != s.minutes OR
                    c.goal != s.goal OR
                    c.assist != s.assist OR
                    c.xG != s.xG OR
                    c.xAG != s.xAG OR
                 )
                 """)
    conn.execute("""
        INSERT INTO core.player_info(club_name, player_name, match_played, age, nationality, minutes, position,goal, assist, xG, xAG, season)
        SELECT 
            s.club_name, s.player_name, s.match_played, s.age, s.nationality, s.minutes, s.position, s.goal, s.assist, s.xG, s.xAG, s.season
        FROM staging.player_info s
        LEFT JOIN core.player_info c
            ON s.club_name = c.club_name AND s.player_name = c.player_name AND s.season = c.season
        WHERE (
            c.player_name IS NULL OR
            c.match_played IS DISTINCT FROM s.match_played OR
            c.minutes IS DISTINCT FROM s.minutes OR
            c.goal IS DISTINCT FROM s.goal OR
            c.assist IS DISTINCT FROM s.assist OR
            c.xG IS DISTINCT FROM s.xG 
            )
                 """)
    
#LOADING TO player_attacking_fact
attacking_count = conn.execute("SELECT COUNT(*) FROM staging.player_attacking_fact").fetchone()[0]
if attacking_count > 0:
    conn.execute("""
        UPDATE core.player_attacking_fact c
        SET valid_to = make_date(s.year, s.month, s.day), is_current = FALSE
            FROM (
                 SELECT * FROM staging.player_attacking_fact
                 ) s
            WHERE c.club_name = s.club_name
                 AND c.player_name = s.player_name
                 AND c.is_current = TRUE 
                 AND (
                    c.sca != s.sca OR
                    c.sca90 != s.sca90 OR
                    c.gca != s.gca OR
                    c.gca90 != s.gca90 OR
                    c.passlive != s.passlive

                 )
                 """)
    conn.execute("""
        INSERT INTO core.player_attacking_fact(club_name, player_name, sca, sca90, gca, gca90, passlive, season)
        SELECT 
            s.club_name, s.player_name, s.sca, s.sca90, s.gca, s.gca90, s.passlive, s.season
        FROM staging.player_attacking_fact s
        LEFT JOIN core.player_attacking_fact c
            ON s.club_name = c.club_name AND s.player_name = c.player_name AND s.season = c.season
        WHERE (
            c.player_name IS NULL OR
            c.sca IS DISTINCT FROM s.sca OR
            c.sca90 IS DISTINCT FROM s.sca90 OR
            c.gca IS DISTINCT FROM s.gca OR
            c.gca90 IS DISTINCT FROM s.gca90 OR
            c.passlive IS DISTINCT FROM s.passlive 
            )
                 """)

#LOADING TO player_defending_fact
player_defending_fact_count = conn.execute("SELECT COUNT(*) FROM staging.player_defending_fact").fetchone()[0]
if player_defending_fact_count > 0:
    conn.execute("""
        UPDATE core.player_defending_fact c
        SET valid_to = make_date(s.year, s.month, s.day), is_current = FALSE
            FROM (
                 SELECT * FROM staging.player_defending_fact
                 ) s
            WHERE c.club_name = s.club_name
                 AND c.player_name = s.player_name
                 AND c.is_current = TRUE 
                 AND (
                    c.tkl != s.tkl OR
                    c.tklWon != s.tklWon OR
                    c.tkl_pct != s.tkl_pct OR
                    c.interception != s.interception OR
                    c.blocks != s.blocks OR
                    c.errors != s.errors

                 )
                 """)
    conn.execute("""
        INSERT INTO core.player_defending_fact(club_name, player_name, tkl, tklWon, tkl_pct, interception, blocks, errors, season)
        SELECT 
            s.club_name, s.player_name, s.tkl, s.tklWon, s.tkl_pct, s.interception, s.blocks, s.errors, s.season
        FROM staging.player_defending_fact s
        LEFT JOIN core.player_defending_fact c
            ON s.club_name = c.club_name AND s.player_name = c.player_name AND s.season = c.season
        WHERE (
            c.player_name IS NULL OR
            c.tkl IS DISTINCT FROM s.tkl OR
            c.tklWon IS DISTINCT FROM s.tklWon OR
            c.tkl_pct IS DISTINCT FROM s.tkl_pct OR
            c.interception IS DISTINCT FROM s.interception OR
            c.blocks IS DISTINCT FROM s.blocks 
            c.errors IS DISTINCT FROM s.errors
            )
                 """)

#LOADING TO player_salary
player_salary_count = conn.execute("SELECT COUNT(*) FROM staging.player_salary").fetchone()[0]
if player_salary_count > 0:
    conn.execute("""
        UPDATE core.player_salary c
        SET valid_to = make_date(s.year, s.month, s.day), is_current = FALSE
            FROM (
                 SELECT * FROM staging.player_salary
                 ) s
            WHERE c.club_name = s.club_name
                 AND c.player_name = s.player_name
                 AND c.is_current = TRUE 
                 AND (
                    c.gross_week != s.gross_week OR
                    c.gross_year != s.gross_year OR
                    c.signed != s.signed OR
                    c.expiration != s.expiration OR
                    c.gross_remaining != s.gross_remaining OR
                    c.release_clause != s.release_clause
                 )
                 """)
    conn.execute("""
        INSERT INTO core.player_salary(club_name, player_name, league, gross_week, gross_year, signed, expiration, gross_remaining, release_clause, season)
        SELECT 
            s.club_name, s.player_name, s.league, s.gross_week, s.gross_year, s.signed, s.expiration,
                  s.gross_remaining, s.release_clause, s.season
        FROM staging.player_salary s
        LEFT JOIN core.player_salary c
            ON s.club_name = c.club_name AND s.player_name = c.player_name AND s.season = c.season
        WHERE (
            c.player_name IS NULL OR
            c.gross_week IS DISTINCT FROM s.gross_week OR
            c.gross_year IS DISTINCT FROM s.gross_year OR
            c.signed IS DISTINCT FROM s.signed OR
            c.expiration IS DISTINCT FROM s.expiration OR
            c.gross_remaining IS DISTINCT FROM s.gross_remaining OR 
            c.release_clause IS DISTINCT FROM s.release_clause
            )
                 """)
    
print("xong")