{{ config(materialized='table') }}

SELECT
    CAST("Id" AS TEXT) AS "Id",
    COALESCE(TRIM("Name"), 'Unknown Opportunity') AS "Name",

    CASE LOWER(TRIM(COALESCE("StageName", '')))
        WHEN 'closed won'              THEN 'Closed Won'
        WHEN 'gewonnen'                THEN 'Closed Won'
        WHEN 'won'                     THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)'  THEN 'Closed Won'
        WHEN 'closed lost'             THEN 'Closed Lost'
        WHEN 'verloren'                THEN 'Closed Lost'
        WHEN 'lost'                    THEN 'Closed Lost'
        WHEN 'abgeschlossen (verloren)'  THEN 'Closed Lost'
        WHEN 'prospecting'             THEN 'Prospecting'
        WHEN 'prospect'                THEN 'Prospecting'
        WHEN 'qualification'           THEN 'Qualification'
        WHEN 'quali'                   THEN 'Qualification'
        WHEN 'qualifikation'           THEN 'Qualification'
        WHEN 'in prüfung'              THEN 'Needs Analysis'
        WHEN 'in kontakt'              THEN 'Qualification'
        ELSE INITCAP(TRIM(COALESCE("StageName", '')))
    END AS "StageName",

    CASE
        WHEN TRIM("CloseDate") IS NULL
             OR LOWER(TRIM(COALESCE("CloseDate", ''))) IN ('n/a', '', '0000-00-00')
            THEN NULL
        WHEN TRIM("CloseDate") ~ '^\d{8}$'
            THEN TO_CHAR(TO_DATE(TRIM("CloseDate"), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM("CloseDate") ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_CHAR(TO_DATE(TRIM("CloseDate"), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM("CloseDate") ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM("CloseDate"), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM("CloseDate") ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM("CloseDate"), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",

    CASE
        WHEN UPPER(TRIM(COALESCE("Amount", ''))) = 'NONE'
             OR TRIM(COALESCE("Amount", '')) = ''
            THEN NULL
        ELSE
            CASE
                -- European format: digits.digits,digits (e.g. 404.415,29)
                WHEN REGEXP_REPLACE(
                         TRIM(COALESCE("Amount", '')),
                          '(?i)[€$£¥]|(?:EUR|USD|GBP|CHF|DOLLARS?)\s*', '', 'g'
                     ) ~ '^\-?\d{1,3}(\.\d{3})+,\d+$'
                    THEN CAST(
                             REPLACE(
                                 REPLACE(
                                     REGEXP_REPLACE(
                                         TRIM(COALESCE("Amount", '')),
                                          '(?i)[€$£¥]|(?:EUR|USD|GBP|CHF|DOLLARS?)\s*', '', 'g'
                                     ),
                                     '.', ''   -- remove thousand-sep dots
                                 ),
                                 ',', '.'     -- swap comma to decimal point
                             )
                         AS DOUBLE PRECISION)
                -- US/plain format: digits.digits or digits (e.g. -426811.82, 0)
                WHEN REGEXP_REPLACE(
                         TRIM(COALESCE("Amount", '')),
                          '(?i)[€$$£¥]|(?:EUR|USD|GBP|CHF|DOLLARS?)\s*', '', 'g'
                     ) ~ '^\-?\d{1,3}(.\d{3})+(,\d+)?|\d+\.\d+$'
                    THEN CAST(
                             REGEXP_REPLACE(
                                 REGEXP_REPLACE(
                                     TRIM(COALESCE("Amount", '')),
                                      '(?i)[€$£¥]|(?:EUR|USD|GBP|CHF|DOLLARS?)\s*', '', 'g'
                                 ),
                                 '[,\s]', ''   -- remove commas and spaces
                             )
                         AS DOUBLE PRECISION)
                ELSE NULL
            END
    END AS "Amount",

    CASE UPPER(TRIM(COALESCE("CurrencyIsoCode", '')))
        WHEN 'EUR' THEN 'EUR'
        WHEN '€'   THEN 'EUR'
        WHEN 'USD' THEN 'USD'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'CHF' THEN 'CHF'
        WHEN 'DOLLAR' THEN 'USD'
        ELSE NULL
    END AS "CurrencyIsoCode",

    CAST("AccountId" AS TEXT)  AS "AccountId",
    NULL::TEXT                 AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT         AS "CreatedDate",
    CURRENT_DATE::TEXT         AS "LastModifiedDate",
    0                          AS "IsDeleted"

FROM {{ source('fixture_messy_data_src', 'Opportunity') }}