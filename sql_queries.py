MetaData = {
        "value i94cntyl":"Lui94cntyl",
        "value $i94prtl":"Lui94prtl",
        "value i94model":"Lui94mode",
        "value i94addrl":"Lui94addrl"
    }


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
        ) VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(city,state,race) DO UPDATE SET 
        mid_age=%s,
        male_population=%s,
        female_population=%s,
        total_population=%s,
        num_of_verterans=%s,
        foreign_born=%s,
        avg_household_size=%s,
        state_cd=%s
""")



airport_insert = ("""
        INSERT INTO airport_insert
        (
            airport_id,
            type,
            name,
            region ,
            city
        ) 
        VALUES
        (%s,%s,%s,%s,%s)
        ON CONFLICT(airport_id) DO UPDATE SET 
        airport_id=%s,
        type=%s,
        name=%s,
        region=%s,
        city=%s
""")


tempreture_insert = ("""
        INSERT INTO airport_insert
        (
            date,
            city,
            country,
            latitude,
            longitude,
            avg_temp,
            avg_temp_uncertain
        ) 
        VALUES
        (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT( date,city,country,latitude,longitude) DO UPDATE SET 
        avg_temp=%s,
        avg_temp_uncertain=%s
""")

immigration_insert = ("""
        INSERT INTO Demographics
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
        ) VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(cicid) DO UPDATE SET 
        i94yr=%s,
        i94mon=%s,
        i94port=%s,
        i94cit=%s,
        i94res=%s,
        arrdate=%s,
        depdate=%s,
        i94addr=%s,
        biryear=%s,
        gender=%s,
        airline=%s,
        visatype=%s
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


dic_query = {
    "Lui94cntyl":Lui94cntyl_insert,
    "Lui94prtl":Lui94prtl_insert,
    "Lui94mode":Lui94mode_insert,
    "Lui94addrl":Lui94addrl_insert
    
}


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