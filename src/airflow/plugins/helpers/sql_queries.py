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

    # Create Ports Dimension Table
    create_ports_table = """
    CREATE TABLE public.dim_ports
    (
    port_id BIGINT GENERATED ALWAYS AS IDENTITY,
    port_code VARCHAR(3) UNIQUE,
    port_city VARCHAR(256),
    port_state VARCHAR(50),
    PRIMARY KEY(port_id)
    )
    """

    # Create Airports Dimension Table
    create_airports_table = """
    CREATE TABLE public.dim_airports
    (
    airport_id BIGINT GENERATED ALWAYS AS IDENTITY,
    port_id BIGINT UNIQUE,
    airport_type VARCHAR(256),
    airport_name VARCHAR(256),
    elevation_ft INT,
    municipality VARCHAR(256),
    gps_code VARCHAR(256),
    iata_code VARCHAR(256),
    local_code VARCHAR(256),
    coordinates VARCHAR(256),
    PRIMARY KEY(airport_id),
    CONSTRAINT fk_port
    FOREIGN KEY(port_id) REFERENCES dim_ports(port_id)
    )
    """

    # Extract ports from staging immigration data
    extract_ports = """
    INSERT INTO public.dim_ports (port_code, port_city, port_state)
    (SELECT DISTINCT p.port_code, p.city, p.state
    FROM public.staging_immigration i
    INNER JOIN public.staging_ports p ON i.port_code = p.port_code
    ORDER BY p.port_code)
    ON CONFLICT (port_code) DO NOTHING
    """