{{ config(materialized='table') }}

SELECT
    opp_kennung AS "Id",
    COALESCE(TRIM(titel), opp_kennung) AS "Name", -- Name is NOT NULL
    CASE
        WHEN LOWER(TRIM(vertriebsphase)) IN ('prospecting', 'akquise') THEN 'Prospecting'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('qualification', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('id. decision makers', 'entscheider identifiziert') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('proposal/price quote', 'angebot/preisangebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('negotiation/review', 'verhandlung/prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('closed won', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('closed lost', 'verloren') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to a valid stage if not matched, as it's NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            TO_DATE(TRIM(zieldatum), 'YYYY-MM-DD'), 'YYYY-MM-DD'
        ),
        TO_CHAR(
            TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD'
        ),
        TO_CHAR(
            TO_DATE(TRIM(zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD'
        ),
        '9999-12-31' -- Default for NOT NULL CloseDate if all parsing fails
    ) AS "CloseDate",
    CASE
        WHEN TRIM(auftragswert) ~ '^-?\d{1,3}(\.\d{3})*(,\d+)?$' THEN
            REPLACE(REPLACE(TRIM(auftragswert), '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN TRIM(auftragswert) ~ '^-?\d+(\.\d+)?$' THEN
            TRIM(auftragswert)::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    TRIM(waehrungscode) AS "CurrencyIsoCode",
    kunden_ref AS "AccountId", -- Maps to Account.Id
    opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
