{{ config(materialized='table') }}

WITH opp_cleaned AS (
    SELECT
        o."opp_kennung",
        o."titel",
        CASE UPPER(TRIM(o."vertriebsphase"))
            WHEN 'PROSPECTING' THEN 'Prospecting'
            WHEN 'PROSPECT' THEN 'Prospecting'
            WHEN 'IN KONTAKT' THEN 'Prospecting'
            WHEN 'QUALIFICATION' THEN 'Qualification'
            WHEN 'QUALIFIKATION' THEN 'Qualification'
            WHEN 'QUALI' THEN 'Qualification'
            WHEN 'IN PRÜFUNG' THEN 'Qualification'
            WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
            WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
            WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
            WHEN 'IDENTIFY DECISION MAKERS' THEN 'Id. Decision Makers'
            WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
            WHEN 'PERZEPTIONSANALYSE' THEN 'Perception Analysis'
            WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
            WHEN 'PROPOSE/PRICING' THEN 'Proposal/Price Quote'
            WHEN 'ANGEBOT' THEN 'Proposal/Price Quote'
            WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
            WHEN 'VERHANDLUNG' THEN 'Negotiation/Review'
            WHEN 'NEGOTIATION' THEN 'Negotiation/Review'
            WHEN 'CLOSED WON' THEN 'Closed Won'
            WHEN 'GEWONNEN' THEN 'Closed Won'
            WHEN 'ABGESCHLOSSEN (GEWONNEN)' THEN 'Closed Won'
            WHEN 'CLOSED-WON' THEN 'Closed Won'
            WHEN 'CLOSEDWON' THEN 'Closed Won'
            WHEN 'WON' THEN 'Closed Won'
            WHEN 'CLOSED LOST' THEN 'Closed Lost'
            WHEN 'VERLOREN' THEN 'Closed Lost'
            WHEN 'ABGESCHLOSSEN (VERLOREN)' THEN 'Closed Lost'
            WHEN 'CLOSED-LOST' THEN 'Closed Lost'
            WHEN 'CLOSEDLOST' THEN 'Closed Lost'
            WHEN 'LOST' THEN 'Closed Lost'
            ELSE NULL
        END AS stage_name,
        CASE
            WHEN o."zieldatum" IS NULL OR TRIM(o."zieldatum") = '' THEN NULL
            WHEN o."zieldatum" ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(o."zieldatum" AS DATE)
            WHEN o."zieldatum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(o."zieldatum"), 'DD.MM.YYYY')
            WHEN o."zieldatum" ~ '^\d{8}$' THEN TO_DATE(TRIM(o."zieldatum"), 'YYYYMMDD')
            WHEN o."zieldatum" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(o."zieldatum"), 'MM/DD/YYYY')
            ELSE NULL
        END AS close_date_raw,
        CASE
            WHEN TRIM(COALESCE(o."auftragswert", '')) IN ('', 'None', 'null', 'NULL', 'N/A') THEN NULL
            ELSE
                CAST(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(TRIM(o."auftragswert"), '[A-Za-z\s€£$]+', '', 'g'),
                              '\\.', ''),
                         ',', '.') AS DOUBLE PRECISION)
        END AS amount,
        o."waehrungscode",
        CASE 
            WHEN o."kunden_ref" IS NOT NULL AND o."kunden_ref" ~ '\d+' 
            THEN CAST(SUBSTRING(o."kunden_ref" FROM '\d+') AS INTEGER)
            ELSE NULL
        END AS opp_numeric_key
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
)

SELECT
    CAST(oc."opp_kennung" AS TEXT) AS "Id",
    INITCAP(TRIM(oc."titel")) AS "Name",
    oc.stage_name AS "StageName",
    TO_CHAR(oc.close_date_raw, 'YYYY-MM-DD') AS "CloseDate",
    CASE UPPER(TRIM(oc."waehrungscode"))
        WHEN 'USD' THEN 'USD'
        WHEN 'EUR' THEN 'EUR'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'CHF' THEN 'CHF'
        WHEN '$' THEN 'USD'
        WHEN 'DOLLAR' THEN 'USD'
        WHEN '€' THEN 'EUR'
        WHEN 'EURO' THEN 'EUR'
        WHEN '£' THEN 'GBP'
        ELSE UPPER(TRIM(oc."waehrungscode"))
    END AS "CurrencyIsoCode",
    CASE 
        WHEN oc.opp_numeric_key IS NOT NULL 
        THEN '001' || LPAD(CAST(oc.opp_numeric_key AS TEXT), 7, '0')
        ELSE NULL
    END AS "AccountId",
    oc."opp_kennung" AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opp_cleaned oc
WHERE oc.stage_name IS NOT NULL;