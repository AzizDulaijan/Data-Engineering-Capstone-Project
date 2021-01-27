import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_i94_table_drop = "DROP table IF EXISTS staging_i94"
tourists_table_drop = "DROP table IF EXISTS tourists"
flights_table_drop = "DROP table IF EXISTS flights"
states_table_drop = "DROP table IF EXISTS states"
stay_durations_table_drop = "DROP table IF EXISTS stay_durations"

# CREATE TABLES


staging_i94_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_i94(
        cicid FLOAT PRIMARY KEY, 
        i94yr NUMERIC,
        i94mon NUMERIC,
        i94cit VARCHAR, 
        i94res VARCHAR,
        i94port VARCHAR,
        arrdate NUMERIC,
        i94mode VARCHAR,
        i94addr VARCHAR,
        depdate NUMERIC,
        i94bir NUMERIC,
        i94visa NUMERIC,
        count NUMERIC,
        dtadfile NUMERIC,
        visapost VARCHAR,
        occup VARCHAR,
        entdepa VARCHAR,
        entdepd VARCHAR,
        entdepu VARCHAR,
        matflag VARCHAR,
        biryear NUMERIC,
        dtaddto VARCHAR,
        gender VARCHAR,
        insnum VARCHAR,
        airline VARCHAR,
        admnum NUMERIC,
        fltno VARCHAR,
        visatype VARCHAR
    );
""")

tourists_table_create = ("""
    CREATE TABLE IF NOT EXISTS tourists(
		cicid NUMERIC PRIMARY KEY, 
		i94cit VARCHAR NOT NULL, 
		i94res VARCHAR NOT NULL, 
		i94mode VARCHAR NOT NULL, 
		visapost VARCHAR, 
		biryear NUMERIC, 
		gender VARCHAR,
		visatype VARCHAR
    );
""")

flights_table_create = ("""
    CREATE TABLE IF NOT EXISTS flights(
		fltno VARCHAR PRIMARY KEY,
		airline VARCHAR, 
		i94port VARCHAR
	);
""")

states_table_create = ("""
    CREATE TABLE IF NOT EXISTS states(
		state_code VARCHAR PRIMARY KEY,
		state VARCHAR NOT NULL, 
        male_pop NUMERIC NOT NULL,
		female_pop NUMERIC NOT NULL,
		total_pop NUMERIC NOT NULL,
		foreign_born_num NUMERIC NOT NULL,
		median_Age NUMERIC NOT NULL
	);
""")

stay_durations_table_create = ("""
    CREATE TABLE IF NOT EXISTS stay_durations (
		cicid NUMERIC PRIMARY KEY,
		fltno VARCHAR NOT NULL,
        arr_state_code VARCHAR NOT NULL, 
		arrdate DATE NOT NULL,
		depdate DATE NOT NULL
	);
""")

# STAGING TABLES

staging_i94_copy = (""" copy staging_i94 
    FROM {data_bucket}
    credentials 'aws_iam_role={role_arn}'
	delimiter ',' removequotes
	IGNOREHEADER 1
    region 'us-east-2' ;
    """).format(data_bucket=config['S3']['LOG_DATA'], 
                role_arn=config['IAM_ROLE']['ARN'])

# region 'us-west-2' 

# INSERT RECORDS

tourists_table_insert = ("""
    INSERT INTO tourists( 
    cicid, i94cit, i94res, 
    i94mode,visapost, biryear, gender, visatype)
        SELECT DISTINCT 
        cicid, i94cit, i94res, 
        i94mode, visapost, biryear,
        gender, visatype
        FROM staging_i94
        WHERE cicid IS NOT NULL
        AND i94mode IS NOT NULL
        AND i94visa = 2.0
        AND cicid NOT IN (SELECT cicid 
                         from tourists)
""")

flights_table_insert = ("""
    INSERT INTO flights 
    (fltno, airline, i94port)
        SELECT DISTINCT
        fltno, airline, i94port
        FROM staging_i94
        WHERE fltno IS NOT NULL
        AND fltno != 'LAND' AND fltno != 'SEA'
        AND i94visa = 2.0
        AND fltno NOT IN (SELECT fltno 
                         from flights)
""")

states_table_insert = ("""
    INSERT INTO states(
		state_code, state, total_pop,
        male_pop, female_pop, 
		foreign_born_num, median_Age)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

stay_durations_table_insert = ("""
    INSERT INTO stay_durations 
    (cicid, fltno,arr_state_code, arrdate, depdate)
        SELECT DISTINCT
        cicid, fltno, i94addr,
        DATEADD(day, CAST(arrdate AS int) ,'1960-1-1'),
        DATEADD(day, CAST(depdate AS int) ,'1960-1-1')
        FROM staging_i94
        WHERE fltno IS NOT NULL 
        AND cicid IS NOT NULL
        AND i94mode IS NOT NULL
        AND i94visa = 2.0
        AND i94addr IS NOT NULL
        AND depdate IS NOT NULL
        AND cicid NOT IN (SELECT cicid 
                         from stay_durations)
""")



# QUERY LISTS
create_table_queries = [staging_i94_table_create, tourists_table_create, flights_table_create, states_table_create, stay_durations_table_create]
drop_table_queries = [staging_i94_table_drop, tourists_table_drop, flights_table_drop, states_table_drop, stay_durations_table_drop]
copy_table_queries = staging_i94_copy
insert_table_queries = [tourists_table_insert, flights_table_insert, stay_durations_table_insert]