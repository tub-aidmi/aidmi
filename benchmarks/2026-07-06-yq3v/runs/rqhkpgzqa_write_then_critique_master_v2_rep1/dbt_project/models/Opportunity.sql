-- noinspection SqlNoDataSourceInspection
{{ config(materialized='table') }}

WITH cleaned_opportunities AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }}
),
parsed_amounts AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        waehrungscode,
        kunden_ref,
        -- First, remove any non-numeric characters except for digits, comma, and dot
        TRIM(REGEXP_REPLACE(auftragswert, '[^0-9,.-]', '', 'g')) AS cleaned_amount_string
    FROM
        cleaned_opportunities
),
transformed_data AS (
    SELECT
        opp_kennung AS "Id",
        COALESCE(titel, 'Untitled Opportunity') AS "Name",
        CASE
            WHEN LOWER(vertriebsphase) IN ('won', 'closed won', 'abgeschlossen (gewonnen)', 'gewonnen', 'closedwon') THEN 'Closed Won'
            WHEN LOWER(vertriebsphase) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)', 'closedlost') THEN 'Closed Lost'
            WHEN LOWER(vertriebsphase) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
            WHEN LOWER(vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
            WHEN LOWER(vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review'
            ELSE NULL -- Will be coalesced later
        END AS "StageName_uncoalesced",
        CASE
            WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
            WHEN zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS "CloseDate_uncoalesced",
        CASE
            WHEN cleaned_amount_string IS NULL THEN NULL
            WHEN cleaned_amount_string LIKE '%,%' AND cleaned_amount_string LIKE '%.%' THEN -- European: 1.234,56
                REPLACE(REPLACE(cleaned_amount_string, '.', ''), ',', '.')::DOUBLE PRECISION
            WHEN cleaned_amount_string LIKE '%,%' THEN -- European: 1234,56
                REPLACE(cleaned_amount_string, ',', '.')::DOUBLE PRECISION
            WHEN cleaned_amount_string ~ '^-?\d+(\.\d+)?$' THEN -- Standard: 1234.56 or 1234
                cleaned_amount_string::DOUBLE PRECISION
            ELSE NULL -- Cannot parse
        END AS "Amount",
        CASE
            WHEN LOWER(waehrungscode) IN ('eur', 'euro', '€') THEN 'EUR'
            WHEN LOWER(waehrungscode) IN ('usd', '$') THEN 'USD'
            WHEN LOWER(waehrungscode) IN ('chf') THEN 'CHF'
            ELSE NULL
        END AS "CurrencyIsoCode",
        kunden_ref AS "AccountId",
        opp_kennung AS "Legacy_Opportunity_ID__c",
        NULL AS "CreatedDate",
        NULL AS "LastModifiedDate",
        0 AS "IsDeleted"
    FROM
        parsed_amounts
)
SELECT
    "Id",
    "Name",
    COALESCE("StageName_uncoalesced", 'Prospecting') AS "StageName",
    COALESCE("CloseDate_uncoalesced", '1900-01-01') AS "CloseDate",
    "Amount",
    "CurrencyIsoCode",
    "AccountId",
    "Legacy_Opportunity_ID__c",
    "CreatedDate",
    "LastModifiedDate",
    "IsDeleted"
FROM
    transformed_data;