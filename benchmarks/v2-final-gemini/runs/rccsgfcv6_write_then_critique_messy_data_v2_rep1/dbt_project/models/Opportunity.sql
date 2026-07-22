-- models/Opportunity.sql

{{ config(materialized='table') }}

WITH cleaned_opportunity_data AS (
    SELECT
        TRIM(opportunity.id) AS id,
        TRIM(opportunity.name) AS name,
        TRIM(opportunity.stagename) AS stagename,
        TRIM(opportunity.closedate) AS closedate,
        -- Pre-process amount string once to remove common non-numeric characters and convert to lower case
        TRIM(REPLACE(REPLACE(LOWER(opportunity.amount), 'eur', ''), ' ', '')) AS cleaned_amount_str,
        TRIM(opportunity.currencyisocode) AS currencyisocode,
        TRIM(opportunity.accountid) AS accountid
    FROM
        {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opportunity
)
SELECT
    id AS "Id",
    COALESCE(name, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(stagename) IN ('prospecting', 'prospect', 'in kontakt', 'prospecting') THEN 'Prospecting'
        WHEN LOWER(stagename) IN ('qualification', 'qualif', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(stagename) IN ('needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(stagename) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(stagename) IN ('id. decision makers', 'id decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(stagename) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(stagename) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(stagename) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(stagename) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)', 'geschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(stagename) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)', 'geschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL enum
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(DATE '1900-01-01', 'YYYY-MM-DD') -- Default for NOT NULL, ensures consistent type
    ) AS "CloseDate",
    NULLIF(
        CASE
            -- European format: contains comma, and if dots are present, they appear before the comma (e.g., 1.234,56)
            WHEN cleaned_amount_str ~ '^\d{1,3}(\.\d{3})*,\d+$' THEN
                REPLACE(REPLACE(cleaned_amount_str, '.', ''), ',', '.')
            -- European format: contains only comma as decimal separator (e.g., 123,45)
            WHEN cleaned_amount_str ~ '^\d+,\d+$' THEN
                REPLACE(cleaned_amount_str, ',', '.')
            -- Otherwise, assume standard or US format (e.g., 1,234.56 or 1234.56)
            ELSE
                cleaned_amount_str
        END
    , '')::DOUBLE PRECISION AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunity_data