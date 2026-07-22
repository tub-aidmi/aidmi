{{ config(materialized='table') }}

SELECT
    t2.opp_kennung AS "Id",
    COALESCE(t2.titel, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(t2.vertriebsphase)) IN ('won', 'closed won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(t2.vertriebsphase)) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(t2.vertriebsphase)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(t2.vertriebsphase)) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(t2.vertriebsphase)) IN ('in prüfung') THEN 'Needs Analysis'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN t2.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(t2.zieldatum AS DATE)
                WHEN t2.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(t2.zieldatum, 'DD.MM.YYYY')
                WHEN t2.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(t2.zieldatum, 'MM/DD/YYYY')
                WHEN t2.zieldatum ~ '^\d{8}$' THEN TO_DATE(t2.zieldatum, 'YYYYMMDD')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL target
    ) AS "CloseDate",
    CASE
        WHEN t2.auftragswert IS NULL THEN NULL
        ELSE
            CASE
                WHEN REGEXP_REPLACE(t2.auftragswert, '[^0-9\.]+', '', 'g') = '' THEN NULL
                ELSE CAST(REGEXP_REPLACE(t2.auftragswert, '[^0-9\.]+', '', 'g') AS DOUBLE PRECISION)
            END
    END AS "Amount",
    t2.waehrungscode AS "CurrencyIsoCode",
    t2.kunden_ref AS "AccountId",
    t2.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS t2
