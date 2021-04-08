
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
        city varchar,
        PRIMARY KEY (airport_id)
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
        PRIMARY KEY (i94citres)
    );

    CREATE TABLE Lui94prtl
    (
        i94port varchar,
        city varchar,
        state varchar,
        PRIMARY KEY (i94port)
    );

    CREATE TABLE Lui94mode
    (
        i94mode int,
        type varchar,
        PRIMARY KEY (i94mode)
    );

    CREATE TABLE Lui94addrl
    (
        i94addr varchar,
        name varchar,
        PRIMARY KEY (i94addr)
    )


""")


