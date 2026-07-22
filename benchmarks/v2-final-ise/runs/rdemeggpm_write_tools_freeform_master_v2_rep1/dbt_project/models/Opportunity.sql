{{ config(materialized='table') }}

SELECT
     -- Salesforce-style Opportunity Id: prefix with 'O' for cross-table FK consistency
     'O' || TRIM("opp_kennung") AS "Id",
    INITCAP(TRIM("titel")) AS "Name",
     -- Map vertriebsphase to target stage enum, bilingual matching
    CASE
        WHEN UPPER(TRIM("vertriebsphase")) = 'PROSPEKTIERUNG' THEN 'Prospecting'
        WHEN UPPER(TRIM("vertriebsphase")) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM("vertriebsphase")) = 'QUALIFIZIERUNG' THEN 'Qualification'
        WHEN UPPER(TRIM("vertriebsphase")) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM("vertriebsphase")) = 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN UPPER(TRIM("vertriebsphase")) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM("vertriebsphase")) = 'WERTPROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM("vertriebsphase")) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM("vertriebsphase")) = 'ENTSCHEIDUNGSFINDER IDENTIFIZIEREN' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM("vertriebsphase")) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM("vertriebsphase")) = 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN UPPER(TRIM("vertriebsphase")) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM("vertriebsphase")) = 'ANGEBOT PREISANGABE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM("vertriebsphase")) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM("vertriebsphase")) = 'VERHANDLUNG' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM("vertriebsphase")) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM("vertriebsphase")) = 'ABSCHLUSS GEWONNEN' THEN 'Closed Won'
        WHEN UPPER(TRIM("vertriebsphase")) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM("vertriebsphase")) = 'ABSCHLUSS VERLOREN' THEN 'Closed Lost'
        WHEN UPPER(TRIM("vertriebsphase")) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
     -- Parse zieldatum (multiple possible formats) into ISO YYYY-MM-DD
    CASE
        WHEN TRIM("zieldatum") IS NULL OR TRIM("zieldatum") = '' THEN NULL
        WHEN TRIM("zieldatum") ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$' THEN TO_DATE(TRIM("zieldatum"), 'DD.MM.YYYY')::TEXT
        WHEN TRIM("zieldatum") ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN SUBSTRING(TRIM("zieldatum") FROM 1 FOR 10)
        WHEN TRIM("zieldatum") ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN TO_DATE(TRIM("zieldatum"), 'MM/DD/YYYY')::TEXT
        WHEN TRIM("zieldatum") ~ '^[0-9]{8}$' THEN
            CASE
                WHEN SUBSTRING(TRIM("zieldatum") FROM 1 FOR 4)::INTEGER BETWEEN 1900 AND 2099
                    THEN TO_DATE(TRIM("zieldatum"), 'YYYYMMDD')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "CloseDate",
     -- Clean and cast amount: strip non-numeric chars, handle European format (1.234,56 -> 1234.56)
    CASE
        WHEN TRIM("auftragswert") IS NULL OR TRIM("auftragswert") = '' THEN NULL
        WHEN REGEXP_REPLACE(TRIM("auftragswert"), '[^0-9]', '', 'g') ~ '^[0-9]+$'
             AND LENGTH(REGEXP_REPLACE(TRIM("auftragswert"), '[^0-9]', '', 'g')) > 0
        THEN
            CAST(
                REGEXP_REPLACE(
                    REPLACE(
                        REGEXP_REPLACE(TRIM("auftragswert"), '[^0-9.,]', '', 'g'),
                         '.', ''
                    ),
                    ',', '.'
                ) AS DOUBLE PRECISION
            )
        ELSE NULL
    END AS "Amount",
    TRIM(UPPER("waehrungscode")) AS "CurrencyIsoCode",
     -- AccountId: Salesforce-style, match Account.Id = 'C' || kundennummer
    CASE
        WHEN TRIM("kunden_ref") IS NOT NULL THEN 'C' || TRIM("kunden_ref")
        ELSE NULL
    END AS "AccountId",
     -- Legacy key from source natural key
    TRIM("opp_kennung") AS "Legacy_Opportunity_ID__c",
     -- Fixed dates
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
