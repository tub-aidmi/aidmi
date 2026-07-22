-- models/Opportunity.sql
{{ config(materialized='table') }}

SELECT
    MD5(opp_kennung) AS "Id",
    COALESCE(TRIM(titel), 'Unknown Opportunity') AS "Name", -- Name is NOT NULL
    CASE
        WHEN LOWER(vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(vertriebsphase) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(vertriebsphase) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(vertriebsphase) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(vertriebsphase) IN ('in prüfung') THEN 'Negotiation/Review' -- Best guess for mapping
        ELSE 'Prospecting' -- Default to Prospecting if not explicitly mapped and NOT NULL target
    END AS "StageName",
    CASE
        WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum::DATE -- YYYY-MM-DD
        WHEN zieldatum ~ '^\d{8}$' THEN TO_DATE(zieldatum, 'YYYYMMDD') -- YYYYMMDD
        WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(zieldatum, 'DD.MM.YYYY') -- DD.MM.YYYY
        WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(zieldatum, 'MM/DD/YYYY') -- M/D/YYYY or MM/DD/YYYY
        ELSE NULL
    END::TEXT AS "CloseDate", -- Target is TEXT, output as ISO YYYY-MM-DD
    CASE
        WHEN TRIM(auftragswert) IS NULL OR TRIM(auftragswert) = '' OR TRIM(auftragswert) = 'None' THEN NULL
        WHEN TRIM(auftragswert) ~ '^[[:space:]]*-?[[:digit:]]+([\.][[:digit:]]{3})*,[[:digit:]]+$' THEN -- European format: dot for thousand, comma for decimal
            CAST(REPLACE(REPLACE(REGEXP_REPLACE(TRIM(auftragswert), '[^0-9,-]+', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE -- Standard format: comma for thousand, dot for decimal or simple decimal
            CAST(REGEXP_REPLACE(TRIM(auftragswert), '[^0-9.-]+', '', 'g') AS DOUBLE PRECISION)
    END AS "Amount",
    TRIM(waehrungscode) AS "CurrencyIsoCode",
    MD5(kunden_ref) AS "AccountId", -- Use consistent Account Id generation
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }}
