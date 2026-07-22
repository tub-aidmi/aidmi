{{ config(materialized='table') }}

SELECT
    mo.opp_kennung AS "Id",
    COALESCE(mo.titel, 'Untitled Opportunity') AS "Name",
    CASE
        WHEN LOWER(mo.vertriebsphase) IN ('won', 'abgeschlossen (gewonnen)', 'gewonnen', 'closed won') THEN 'Closed Won'
        WHEN LOWER(mo.vertriebsphase) IN ('lost', 'verloren', 'abgeschlossen (verloren)', 'closed lost') THEN 'Closed Lost'
        WHEN LOWER(mo.vertriebsphase) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(mo.vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(mo.vertriebsphase) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default value for StageName as it's NOT NULL
    END AS "StageName",
    CASE
        WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN mo.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN mo.zieldatum
        WHEN mo.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE '1900-01-01' -- Default date as CloseDate is NOT NULL and a valid date is required.
    END AS "CloseDate",
    CAST(
        CASE
            WHEN mo.auftragswert IS NULL THEN NULL
            ELSE
                CASE
                    WHEN REGEXP_REPLACE(TRIM(UPPER(mo.auftragswert)), '^(EUR|€|$)\s*', '') ~ '^-?\d+\.\d{3},\d{2}$' THEN -- European format with thousands dot and decimal comma
                        REPLACE(REPLACE(REGEXP_REPLACE(TRIM(UPPER(mo.auftragswert)), '^(EUR|€|$)\s*', ''), '.', ''), ',', '.')
                    WHEN REGEXP_REPLACE(TRIM(UPPER(mo.auftragswert)), '^(EUR|€|$)\s*', '') ~ '^-?\d+,\d{2}$' THEN -- European format with decimal comma (no thousands dot)
                        REPLACE(REGEXP_REPLACE(TRIM(UPPER(mo.auftragswert)), '^(EUR|€|$)\s*', ''), ',', '.')
                    WHEN REGEXP_REPLACE(TRIM(UPPER(mo.auftragswert)), '^(EUR|€|$)\s*', '') ~ '^-?\d+(\.\d+)?$' THEN -- US format or plain number
                        REGEXP_REPLACE(TRIM(UPPER(mo.auftragswert)), '^(EUR|€|$)\s*', '')
                    ELSE
                        NULL -- Unparseable
                END
        END AS DOUBLE PRECISION
    ) AS "Amount",
    mo.waehrungscode AS "CurrencyIsoCode",
    mk.kundennummer AS "AccountId",
    mo.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
ON
    mo.kunden_ref = mk.kundennummer
