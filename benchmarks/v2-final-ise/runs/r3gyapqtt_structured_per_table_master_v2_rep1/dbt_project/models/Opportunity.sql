{{ config(materialized='table') }}

SELECT
    '001' || LPAD(REPLACE("opp_kennung", 'OPP-', ''), 15, '0') AS "Id",
    "titel" AS "Name",
    CASE
        WHEN UPPER(TRIM("vertriebsphase")) IN ('PROSPECTING', 'IN KONTAKT') THEN 'Prospecting'
        WHEN UPPER(TRIM("vertriebsphase")) IN ('QUALIFICATION', 'QUALI') THEN 'Qualification'
        WHEN UPPER(TRIM("vertriebsphase")) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM("vertriebsphase")) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM("vertriebsphase")) IN ('ID. DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM("vertriebsphase")) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM("vertriebsphase")) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM("vertriebsphase")) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM("vertriebsphase")) IN ('CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN UPPER(TRIM("vertriebsphase")) IN ('CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)', 'LOST') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN "zieldatum" ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE("zieldatum", 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN "zieldatum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE("zieldatum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN "zieldatum" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE("zieldatum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN "zieldatum" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE("zieldatum", 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN "auftragswert" IS NULL OR TRIM("auftragswert") = 'None' THEN NULL
        ELSE
            CASE
                WHEN "auftragswert" ~ '^[0-9\-]+\.?[0-9]*$' THEN "auftragswert"::DOUBLE PRECISION
                ELSE
                    CASE
                        WHEN "auftragswert" ~ ',' THEN
                            REGEXP_REPLACE(
                                REGEXP_REPLACE("auftragswert", '[^0-9\-,]', '', 'g'),
                                '(\d+),(\d+)', '\1.\2', 'g'
                            )::DOUBLE PRECISION
                        ELSE
                            REGEXP_REPLACE("auftragswert", '[^0-9\-.]', '', 'g')::DOUBLE PRECISION
                    END
            END
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM("waehrungscode")) IN ('CHF') THEN 'CHF'
        WHEN UPPER(TRIM("waehrungscode")) IN ('EUR', 'EURO', '€') THEN 'EUR'
        WHEN UPPER(TRIM("waehrungscode")) IN ('USD', '$', 'DOLLAR') THEN 'USD'
        WHEN UPPER(TRIM("waehrungscode")) IN ('GBP') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    '001' || LPAD(REPLACE("kunden_ref", 'KD-', ''), 15, '0') AS "AccountId",
    "opp_kennung" AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}