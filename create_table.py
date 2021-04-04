

table_refresh = ("""
    DROP TABLE IF EXISTS Demographics;
    DROP TABLE IF EXISTS airport;
    DROP TABLE IF EXISTS tempreture;
    DROP TABLE IF EXISTS immigration;
    DROP TABLE IF EXISTS Lui94cntyl;
    DROP TABLE IF EXISTS Lui94prtl;
    DROP TABLE IF EXISTS Lui94mode;
    DROP TABLE IF EXISTS Lui94addrl;
    
    CREATE TABLE IF NOT EXISTS Demographics 
    (
        city varchar,
        state varchar,
        race varchar,
        mid_age int,
        male_population int,
        female_population int,
        total_population int,
        num_of_verterans int,
        foreign_born int,
        avg_household_size float,
        state_cd varchar,
        PRIMARY KEY(city, state, race)
    );
    
    CREATE TABLE IF NOT EXISTS airport
    (
        airport_id int,
        type varchar,
        name varchar,
        region varchar,
        city varchar
        PRIMARY KEY(airport_id)
    );
    
    
    CREATE TABLE IF NOT EXISTS tempreture
    (
        date date,
        city varchar,
        country varchar,
        latitude varchar,
        longitude varchar,
        avg_temp float,
        avg_temp_uncertain float,
        PRIMARY KEY (date,city, country, latitude, longitude)
    );
    
    CREATE TABLE IF NOT EXISTS immigration
    (
        cicid bigint,
        i94yr int,
        i94mon int,
        i94port varchar,
        i94cit int,
        i94res int,
        arrdate date,
        depdate date,
        i94addr varchar,
        biryear int,
        gender varchar,
        airline varchar,
        visatype varchar
    );
    
    CREATE TABLE Lui94cntyl
    (
        i94citres int,
        country_name varchar,
        PRIMARY KEY (citres_id)
    );
    
    CREATE TABLE Lui94prtl
    (
        i94port varchar,
        city varchar,
        state varchar
        PRIMARY KEY (i94port)
    );
    
    CREATE TABLE Lui94mode
    (
        i94mode int,
        type varchar
    );
    
    CREATE TABLE Lui94addrl
    (
        i94addr varchar,
        name varchar
    )
    
    
""")

# Insert Records
Demographics_insert = ("""
        INSERT INTO Demographics
        (city,state,race,mid_age,male_population,female_population,total_population,
        num_of_verterans,foreign_born,avg_household_size,state_cd) VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(city,state,race) DO UPDATE SET 
        mid_age=%s,male_population=%s,female_population=%s,total_population=%s,
        num_of_verterans=%s,foreign_born=%s,avg_household_size=%s,state_cd=%s
""")
