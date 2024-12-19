-- provided
SELECT ST_GEOMETRYFROMEWKB('0101000020797F000066666666A9CB17411F85EBC19E325641') g;

-- expected
SELECT
    IF(
        REGEXP_MATCHES('0101000020797F000066666666A9CB17411F85EBC19E325641','^[0-9A-Fa-f]+$')
        , ST_GEOMFROMHEXEWKB('0101000020797F000066666666A9CB17411F85EBC19E325641')
        , ST_GEOMFROMWKB(ENCODE('0101000020797F000066666666A9CB17411F85EBC19E325641')
    )
) g;

-- result
"g"
"POINT (389866.35 5819003.03)"


-- provided
SELECT TO_GEOMETRY('SRID=4326;POINT(1820.12 890.56)') g;

-- expected
SELECT Cast('SRID=4326;POINT(1820.12 890.56)' AS GEOMETRY) g;

-- result
"g"
"POINT (1820.12 890.56)"