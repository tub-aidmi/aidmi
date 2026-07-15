{{ config(materialized='table') }}

SELECT 
    '006' || LPAD(opp_kennung, 15, '0') AS "Id",

    COALESCE(INITCAP(TRIM(titel)), 'Opportunity - ' || opp_kennung) AS "Name",

    CASE LOWER(TRIM(vertriebsphase))
        WHEN 'akquise' THEN 'Prospecting'
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'wertproposition' THEN 'Value Proposition'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'angebot/preisanfrage' THEN 'Proposal/Price Quote'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung/überprüfung' THEN 'Negotiation/Review'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",

    CASE 
        WHEN TRIM(COALESCE(zieldatum, '')) = '' THEN NULL
        WHEN TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
            TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(zieldatum) ~ '^\d{8}$' THEN 
            SUBSTR(TRIM(zieldatum), 1, 4) || '-' || 
            SUBSTR(TRIM(zieldatum), 5, 2) || '-' || 
            SUBSTR(TRIM(zieldatum), 7, 2)
        WHEN TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(zieldatum)
        ELSE NULL
    END AS "CloseDate",

    CASE 
        WHEN TRIM(COALESCE(auftragswert, '')) = '' THEN NULL
        ELSE
            CASE
                -- Guard: if no digits remain after stripping non-numeric chars, return NULL instead of casting an empty string
                WHEN REGEXP_REPLACE(TRIM(auftragswert), '[^\d.,]', '', 'g') NOT LIKE '%[0-9]%' THEN NULL
                ELSE
                    CAST(
                        CASE
                            -- European format: comma with 1-3 trailing digits = decimal separator (e.g. "1.234,56")
                            WHEN REGEXP_REPLACE(TRIM(auftragswert), '[^\d.,]', '', 'g') ~ ',\d{1,3}$' THEN 
                                REPLACE(
                                    REPLACE(REGEXP_REPLACE(TRIM(auftragswert), '[^\d.,]', '', 'g'), '.', ''),
                                    ',', '.'
                                )
                            -- Standard format: plain digits or dot-decimal (e.g. "1234.56")
                            ELSE 
                                REGEXP_REPLACE(TRIM(auftragswert), '[^\d.,]', '', 'g')
                        END
                    AS DOUBLE PRECISION)
            END
    END AS "Amount",

    UPPER(TRIM(waehrungscode)) AS "CurrencyIsoCode",

    '001' || LPAD(TRIM(kunden_ref), 15, '0') AS "AccountId",

    opp_kennung AS "Legacy_Opportunity_ID__c",

    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",

    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}