{{ config(materialized='table') }}

SELECT
    MD5(mo.opp_kennung) AS "Id",
    mo.titel AS "Name",
    CASE
        WHEN LOWER(mo.vertriebsphase) IN ('won', 'closed won', 'gewonnen', 'abgeschlossen (gewonnen)', 'closedwon') THEN 'Closed Won'
        WHEN LOWER(mo.vertriebsphase) IN ('lost', 'verloren', 'abgeschlossen (verloren)', 'closed lost') THEN 'Closed Lost'
        WHEN LOWER(mo.vertriebsphase) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(mo.vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt', 'in prüfung') THEN 'Prospecting'
        ELSE 'Prospecting' -- Default to Prospecting for unmatched values
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(mo.zieldatum, 'DD.MM.YYYY')
                WHEN mo.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(mo.zieldatum, 'MM/DD/YYYY')
                WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(mo.zieldatum, 'YYYY-MM-DD')
                WHEN mo.zieldatum ~ '^\d{8}$' THEN TO_DATE(mo.zieldatum, 'YYYYMMDD')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default to current date if NULL or unparseable
    ) AS "CloseDate",
    CASE
        WHEN mo.auftragswert IS NULL THEN NULL
        ELSE
            CASE
                WHEN TRIM(REPLACE(mo.auftragswert, 'EUR ', '')) LIKE '%,%' THEN
                    REPLACE(REPLACE(TRIM(REPLACE(mo.auftragswert, 'EUR ', '')), '.', ''), ',', '.')::DOUBLE PRECISION
                WHEN TRIM(REPLACE(mo.auftragswert, 'EUR ', '')) ~ '^[+-]?\d+(\.\d+)?$' THEN
                    TRIM(REPLACE(mo.auftragswert, 'EUR ', ''))::DOUBLE PRECISION
                ELSE NULL
            END
    END AS "Amount",
    mo.waehrungscode AS "CurrencyIsoCode",
    MD5(mo.kunden_ref) AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
```