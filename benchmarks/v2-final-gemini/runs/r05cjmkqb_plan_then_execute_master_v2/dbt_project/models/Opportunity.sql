{{ config(materialized='table') }}

SELECT
    MD5(TRIM(mo.opp_kennung)) AS "Id",
    TRIM(mo.titel) AS "Name",
    CASE TRIM(UPPER(mo.vertriebsphase))
        WHEN 'PROSPEKTIERUNG' THEN 'Prospecting'
        WHEN 'QUALIFIKATION' THEN 'Qualification'
        WHEN 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN 'WERTANGEBOT' THEN 'Value Proposition'
        WHEN 'ID. ENTSCHEIDER' THEN 'Id. Decision Makers'
        WHEN 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN 'ANGEBOT/PREISANGEBOT' THEN 'Proposal/Price Quote'
        WHEN 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
        WHEN 'GEWONNEN' THEN 'Closed Won'
        WHEN 'VERLOREN' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL column
    END AS "StageName",
    COALESCE(
        (CASE
            WHEN mo.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN mo.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN mo.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN mo.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(mo.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END),
        '1900-01-01' -- Default for NOT NULL column
    ) AS "CloseDate",
    CASE
        WHEN TRIM(mo.auftragswert) ~ '^-?\d{1,3}(\.\d{3})*,\d+$' OR TRIM(mo.auftragswert) ~ '^-?\d+(\.\d+)?$'
        THEN REPLACE(REPLACE(TRIM(mo.auftragswert), '.', ''), ',', '.')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    TRIM(mo.waehrungscode) AS "CurrencyIsoCode",
    MD5(TRIM(mo.kunden_ref)) AS "AccountId",
    TRIM(mo.opp_kennung) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP AS "CreatedDate",
    CURRENT_TIMESTAMP AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
ON
    TRIM(mo.kunden_ref) = TRIM(mk.kundennummer)
