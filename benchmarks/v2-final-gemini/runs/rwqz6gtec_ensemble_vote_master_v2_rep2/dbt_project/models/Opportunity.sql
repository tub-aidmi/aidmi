{{ config(materialized='table') }}

SELECT
    opp.opp_kennung AS "Id",
    COALESCE(opp.titel, 'Untitled Opportunity') AS "Name",
    CASE
        WHEN LOWER(opp.vertriebsphase) IN ('won', 'closed won', 'abgeschlossen (gewonnen)', 'gewonnen', 'closed won') THEN 'Closed Won'
        WHEN LOWER(opp.vertriebsphase) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)', 'lost') THEN 'Closed Lost'
        WHEN LOWER(opp.vertriebsphase) IN ('qualifikation', 'quali', 'qualification', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(opp.vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        ELSE 'Prospecting' -- Default for NOT NULL field
    END AS "StageName",
    COALESCE(
        CASE
            WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN opp.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN opp.zieldatum -- Already in YYYY-MM-DD format
            WHEN opp.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        CURRENT_DATE::TEXT -- Default for NOT NULL field
    ) AS "CloseDate",
    NULLIF(TRIM(REPLACE(REPLACE(LOWER(REGEXP_REPLACE(opp.auftragswert, '(\$|€|eur|chf|usd|euro)', '', 'g')), '.', ''), ',', '.')), '')::DOUBLE PRECISION AS "Amount",
    UPPER(opp.waehrungscode) AS "CurrencyIsoCode",
    knd.kundennummer AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS knd
    ON SUBSTRING(opp.kunden_ref FROM 'M\d+') = SUBSTRING(knd.kundennummer FROM 'M\d+')
