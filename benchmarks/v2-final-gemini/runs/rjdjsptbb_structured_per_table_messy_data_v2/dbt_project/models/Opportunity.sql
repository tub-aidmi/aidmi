-- This dbt model transforms source data into the Opportunity target schema.

{{ config(materialized='table') }}

SELECT
    opp.id AS "Id",
    COALESCE(TRIM(opp.name), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(opp.stagename)) IN ('won', 'closed won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.stagename)) IN ('lost', 'closed lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(opp.stagename)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.stagename)) IN ('qualification', 'qualifikation', 'quali', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(TRIM(opp.stagename)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opp.stagename)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(opp.stagename)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opp.stagename)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(opp.stagename)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opp.stagename)) = 'negotiation/review' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for unmapped or NULL stagenames, as StageName is NOT NULL
    END AS "StageName",
    CASE
        WHEN opp.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(opp.closedate::DATE, 'YYYY-MM-DD') -- YYYY-MM-DD
        WHEN opp.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
        WHEN opp.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
        WHEN opp.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY or M/D/YYYY
        ELSE '1900-01-01' -- Default for unparseable or NULL dates, as CloseDate is NOT NULL
    END AS "CloseDate",
    CAST(
        CASE
            WHEN TRIM(opp.amount) IS NULL OR TRIM(opp.amount) = '' THEN NULL
            ELSE
                REPLACE(
                    REPLACE(
                        REGEXP_REPLACE(TRIM(opp.amount), '[^0-9\.,-]', '', 'g'), -- Remove all non-numeric, non-dot, non-comma, non-hyphen chars
                        '.', '' -- Remove thousand separators (assuming dot is for thousand if comma exists for decimal)
                    ),
                    ',', '.' -- Replace decimal comma with dot
                )
        END
    AS DOUBLE PRECISION) AS "Amount",
    opp.currencyisocode AS "CurrencyIsoCode",
    opp.accountid AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opp