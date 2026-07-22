{{ config(materialized='table') }}

SELECT
     'O' || SUBSTR(MD5(src."opp_kennung"), 1, 14) AS "Id",
    src."titel" AS "Name",
    CASE
        WHEN UPPER(TRIM(src."vertriebsphase")) = 'PROSPEKTION' THEN 'Prospecting'
        WHEN UPPER(TRIM(src."vertriebsphase")) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(src."vertriebsphase")) = 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(src."vertriebsphase")) = 'WERTPROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(src."vertriebsphase")) = 'ENTSCHEIDUNGSFINDER IDENTIFIZIEREN' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(src."vertriebsphase")) = 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(src."vertriebsphase")) = 'ANGEBOT/PREISZITIERUNG' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(src."vertriebsphase")) = 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(src."vertriebsphase")) = 'GEWONNEN GESCHLOSSEN' THEN 'Closed Won'
        WHEN UPPER(TRIM(src."vertriebsphase")) = 'VERLOREN GESCHLOSSEN' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN src."zieldatum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src."zieldatum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src."zieldatum" ~ '^\d{8}$' THEN TO_CHAR(
            TO_DATE(SUBSTR(src."zieldatum", 1, 4) || '-' || SUBSTR(src."zieldatum", 5, 2) || '-' || SUBSTR(src."zieldatum", 7, 2), 'YYYY-MM-DD'), 'YYYY-MM-DD'
         )
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN src."auftragswert" ~ '^\s*[\d\.,\-+]+$' THEN
            CAST(
                REGEXP_REPLACE(
                    CASE 
                        WHEN REGEXP_REPLACE(src."auftragswert", '[^\d.,\-+]', '', 'g') ~ ',' THEN
                            -- European format: remove all thousand-sep dots, then swap comma to period
                            REGEXP_REPLACE(REGEXP_REPLACE(src."auftragswert", '[^\d.,\-+]', '', 'g'), '\.', '')
                        ELSE
                            -- Standard US/ISO format with only a decimal point
                            REGEXP_REPLACE(src."auftragswert", '[^\d.,\-+]', '', 'g')
                    END,
                    ',', '.'  -- swap decimal comma to period  
                ) AS DOUBLE PRECISION
            )
        ELSE NULL
    END AS "Amount",
    src."waehrungscode" AS "CurrencyIsoCode",
     'A' || SUBSTR(MD5(src."kunden_ref"), 1, 14) AS "AccountId",
    src."opp_kennung" AS "Legacy_Opportunity_ID__c",
     '1970-01-01' AS "CreatedDate",
     '1970-01-01' AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} src
