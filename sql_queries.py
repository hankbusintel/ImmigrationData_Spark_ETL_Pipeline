

# Insert Records
Demographics_insert = ("""
        INSERT INTO Demographics
        (
                city,
                state,
                race,
                mid_age,
                male_population,
                female_population,
                total_population,
                num_of_verterans,
                foreign_born,
                avg_household_size,
                state_cd
        )
        SELECT  city,
                state,
                race,
                mid_age,
                male_population,
                female_population,
                total_population,
                num_of_verterans,
                foreign_born,
                avg_household_size,
                state_cd
        FROM stg_Demographic
        ON CONFLICT(city,state,race) DO UPDATE SET 
        mid_age=EXCLUDED.mid_age,
        male_population=EXCLUDED.male_population,
        female_population=EXCLUDED.female_population,
        total_population=EXCLUDED.total_population,
        num_of_verterans=EXCLUDED.num_of_verterans,
        foreign_born=EXCLUDED.foreign_born,
        avg_household_size=EXCLUDED.avg_household_size,
        state_cd=EXCLUDED.state_cd
""")



airport_insert = ("""
        INSERT INTO airport
        (
            airport_id,
            type,
            name,
            region ,
            city
        ) 
        SELECT 
            airport_id,
            type,
            name,
            region ,
            city
        FROM stg_airport
        ON CONFLICT(airport_id) DO UPDATE SET 
        type=EXCLUDED.type,
        name=EXCLUDED.type,
        region=EXCLUDED.type,
        city=EXCLUDED.type
""")


tempreture_insert = ("""
        INSERT INTO tempreture
        (
            date,
            city,
            country,
            latitude,
            longitude,
            avg_temp,
            avg_temp_uncertain
        ) 
        SELECT date,
                city,
                country,
                latitude,
                longitude,
                avg_temp,
                avg_temp_uncertain
        FROM stg_tempreture
        ON CONFLICT( date,city,country,latitude,longitude) DO UPDATE SET 
        avg_temp=EXCLUDED.avg_temp,
        avg_temp_uncertain=EXCLUDED.avg_temp
""")

immigration_insert = ("""
        INSERT INTO immigration
        (
            cicid,
            i94yr,
            i94mon,
            i94port,
            i94cit,
            i94res,
            arrdate,
            depdate,
            i94addr,
            biryear,
            gender,
            airline,
            visatype
        ) 
        SELECT cicid,
            i94yr,
            i94mon,
            i94port,
            i94cit,
            i94res,
            arrdate,
            depdate,
            i94addr,
            biryear,
            gender,
            airline,
            visatype
        FROM stg_immigration
        ON CONFLICT(cicid) DO UPDATE SET 
        i94yr=EXCLUDED.i94yr,
        i94mon=EXCLUDED.i94mon,
        i94port=EXCLUDED.i94port,
        i94cit=EXCLUDED.i94cit,
        i94res=EXCLUDED.i94res,
        arrdate=EXCLUDED.arrdate,
        depdate=EXCLUDED.depdate,
        i94addr=EXCLUDED.i94addr,
        biryear=EXCLUDED.biryear,
        gender=EXCLUDED.gender,
        airline=EXCLUDED.airline,
        visatype=EXCLUDED.visatype
""")

Lui94cntyl_insert = ("""
        INSERT INTO Lui94cntyl
        (
            i94citres,
            country_name
        ) 
        VALUES
        (%s,%s)
        ON CONFLICT(i94citres) DO UPDATE SET 
        country_name=%s
""")

Lui94prtl_insert = ("""
        INSERT INTO Lui94prtl
        (
            i94port,
            city,
            state
        ) 
        VALUES
        (%s,%s,%s)
        ON CONFLICT(i94port) DO UPDATE SET 
        city=%s,
        state=%s
""")


Lui94mode_insert = ("""
        INSERT INTO Lui94mode
        (
            i94mode,
            type
        ) 
        VALUES
        (%s,%s)
        ON CONFLICT(i94mode) DO UPDATE SET 
        type=%s
""")

Lui94addrl_insert = ("""
        INSERT INTO Lui94addrl
        (
            i94addr,
            name
        ) 
        VALUES
        (%s,%s)
        ON CONFLICT(i94addr) DO UPDATE SET 
        name=%s
""")





spark_clean_demo = ("""
    WITH C AS
        (
            SELECT 
                *, row_number() over (partition by City,State,Race order by `Total Population` desc) as rank
            FROM demo
            WHERE coalesce(City,'') <> ''
            AND coalesce(State,'') <> ''
            AND coalesce(Race,'') <> ''  
        )
        SELECT city, 
               state, 
               race, 
               CAST(`Median age` AS INT) AS mid_age,
               CAST(`Male Population` AS INT) AS male_population,
               CAST(`Female Population` AS INT) as female_population,             
               CAST(`Total Population` AS INT) as total_population,
               CAST(`Number of Veterans` AS INT) as num_of_verterans,
               CAST(`Foreign-born` AS INT) AS foreign_born,
               CAST(`Average Household Size` AS FLOAT) AS avg_household_size,
               `State Code` as state_cd,
               CAST(`Count` AS INT) AS count
        FROM C
        WHERE rank = 1  
""")

spark_clean_airport=("""
    WITH C AS
        (
            SELECT ident ,type,name,iso_region,municipality,
            row_number() over (partition by ident order by `type` desc) as rank
            FROM airport
            WHERE iso_country='US'
            AND coalesce(municipality,'') <> '' 
            AND coalesce(type,'') <> ''
            AND coalesce(name,'') <> ''
            AND coalesce(ISO_region,'') <> ''
            AND coalesce(ident,'') <> ''
        )
        SELECT ident as airport_id,
               type,
               name,
               replace(iso_region,'US-','') AS region,
        municipality as city FROM C
        WHERE rank = 1
""")

spark_clean_temp = ("""
    SELECT           
           distinct 
           cast(dt as date) as date,            
           city,
           country,
           latitude,
           longitude,
           cast(AverageTemperature as float) as avg_temp,
           cast(AverageTemperatureUncertainty AS float) as avg_temp_uncertain
    FROM tempreture
    WHERE coalesce(AverageTemperature,'') <>'' 
    AND coalesce(AverageTemperatureUncertainty,'') <> ''
    AND Country = 'United States'
    order by Date 
    
""")

spark_clean_imrr = ("""
         SELECT
           cast(cicid as bigint),
           cast(i94yr as int),
           cast(i94mon as int),
           i94port, 
           cast(i94cit as int) as i94cit,
           cast(i94res as int) as i94res,
           cast(i94mode as int) as i94mode,
           date_add('1960-01-01',cast(arrdate as int)) as arrdate,
           date_add('1960-01-01',cast(depdate as int)) as depdate,
           i94addr,
           cast(biryear as int) as biryear,
           gender,
           airline,
           visatype 
        FROM immigration
""")

table_refresh = ("""

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
        airport_id varchar,
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
        visatype varchar,
        PRIMARY KEY (cicid)
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