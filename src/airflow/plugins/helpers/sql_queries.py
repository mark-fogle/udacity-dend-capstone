class SqlQueries:
    """
    Contains SQL queries used to build schema and extract data
    """
    # Create Countries Table
    create_countries_table = """
    CREATE TABLE public.dim_countries
    (
    country_id bigint GENERATED ALWAYS AS IDENTITY,
    country_code varchar(3) NOT NULL UNIQUE,
    country varchar(256) NOT NULL UNIQUE,
    PRIMARY KEY(country_id)
    )
    """