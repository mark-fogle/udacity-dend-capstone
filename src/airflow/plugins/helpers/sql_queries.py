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

    # Extract countries from staging immigration data
    extract_countries = """
    INSERT INTO public.dim_countries (country_code, country)
    (SELECT DISTINCT c.country_code, c.country
    FROM public.staging_immigration i
    INNER JOIN public.staging_countries c ON i.country_code = c.country_code
    ORDER BY c.country) 
    ON CONFLICT (country)
    DO NOTHING
    """