CREATE OR REPLACE PROCEDURE TRANSFORM_RAW_PRODUCTS_FULL()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    TRUNCATE TABLE DATA_MART.FACT_PRICE_TRACKING;

  
    INSERT INTO DATA_MART.FACT_PRICE_TRACKING (
        TITLE,
        BRAND,
        PRICE,
        REVIEWS,
        RATINGS,
        SOURCE,
        CATEGORY
    )
    SELECT
        TITLE,
        BRAND,
        TRY_TO_DOUBLE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(PRICE, '[^0-9.,]', ''), 
                    ',', ''                                
                ),
                '\\.{2,}', '.'                            
            )
        ) AS PRICE,
        TRY_TO_NUMBER(
            CASE
                WHEN TRY_TO_DOUBLE(TRIM(REVIEWS)) <= 5
                     AND (POSITION(',' IN RATINGS) > 0 OR LENGTH(REPLACE(RATINGS, ',', '')) >= 4)
                THEN REPLACE(RATINGS, ',', '')
                ELSE REPLACE(REVIEWS, ',', '')
            END
        ) AS REVIEWS,
        ROUND(
            CASE
                WHEN TRY_TO_DOUBLE(
                    CASE
                        WHEN TRY_TO_DOUBLE(TRIM(REVIEWS)) <= 5
                             AND (POSITION(',' IN RATINGS) > 0 OR LENGTH(REPLACE(RATINGS, ',', '')) >= 4)
                        THEN TRIM(REVIEWS)
                        ELSE TRIM(RATINGS)
                    END
                ) > 5 THEN
                    CASE
                        WHEN TRY_TO_DOUBLE(
                            CASE
                                WHEN TRY_TO_DOUBLE(TRIM(REVIEWS)) <= 5
                                     AND (POSITION(',' IN RATINGS) > 0 OR LENGTH(REPLACE(RATINGS, ',', '')) >= 4)
                                THEN TRIM(REVIEWS)
                                ELSE TRIM(RATINGS)
                            END
                        ) / 1000 BETWEEN 1 AND 5
                        THEN TRY_TO_DOUBLE(
                            CASE
                                WHEN TRY_TO_DOUBLE(TRIM(REVIEWS)) <= 5
                                     AND (POSITION(',' IN RATINGS) > 0 OR LENGTH(REPLACE(RATINGS, ',', '')) >= 4)
                                THEN TRIM(REVIEWS)
                                ELSE TRIM(RATINGS)
                            END
                        ) / 1000
                        ELSE 4.1
                    END
                WHEN TRY_TO_DOUBLE(
                    CASE
                        WHEN TRY_TO_DOUBLE(TRIM(REVIEWS)) <= 5
                             AND (POSITION(',' IN RATINGS) > 0 OR LENGTH(REPLACE(RATINGS, ',', '')) >= 4)
                        THEN TRIM(REVIEWS)
                        ELSE TRIM(RATINGS)
                    END
                ) < 1 THEN 4.1
                ELSE TRY_TO_DOUBLE(
                    CASE
                        WHEN TRY_TO_DOUBLE(TRIM(REVIEWS)) <= 5
                             AND (POSITION(',' IN RATINGS) > 0 OR LENGTH(REPLACE(RATINGS, ',', '')) >= 4)
                        THEN TRIM(REVIEWS)
                        ELSE TRIM(RATINGS)
                    END
                )
            END,
        1) AS RATINGS,
        SOURCE,
        CATEGORY
    FROM RAW_DATA
    WHERE TITLE IS NOT NULL
      AND TRIM(LOWER(TITLE)) NOT IN ('n/a', 'na', '')
      AND REVIEWS IS NOT NULL AND RATINGS IS NOT NULL
      AND LOWER(TRIM(REVIEWS)) NOT IN ('na', 'n/a', 'n\\a')
      AND LOWER(TRIM(RATINGS)) NOT IN ('na', 'n/a', 'n\\a')
      AND TRIM(REVIEWS) <> ''
      AND TRIM(RATINGS) <> '';

    RETURN 'Data transformed and loaded into DATA_MART.FACT_PRICE_TRACKING successfully.';
END;
$$;