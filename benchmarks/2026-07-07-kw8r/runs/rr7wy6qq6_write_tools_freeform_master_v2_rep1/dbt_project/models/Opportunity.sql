{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        o.opp_kennung,
        o.titel,
        o.vertriebsphase,
        o.zieldatum,
        o.auftragswert,
        o.waehrungscode,
        o.kunden_ref,
        a.kundennummer AS account_kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} a
        ON o.kunden_ref = a.kundennummer
),

normalized AS (
    SELECT
        opp_kennung,
        INITCAP(TRIM(titel)) AS name,
        TRIM(vertriebsphase) AS stage,
        TRIM(zieldatum) AS close_date,
        TRIM(auftragswert) AS amount,
        UPPER(TRIM(waehrungscode)) AS currency,
        account_kundennummer
    FROM source_data
),

parsed_amount AS (
    SELECT
        opp_kennung,
        name,
        stage,
        close_date,
        CASE
            WHEN amount ~ '^[0-9]+(\.[0-9]{3})*(,[0-9]+)?$' THEN
                -- European format: 1.234,56 -> 1234.56
                REGEXP_REPLACE(REGEXP_REPLACE(amount, '\.', '', 'g'), ',', '.')::DOUBLE PRECISION
            WHEN amount ~ '^[0-9]+(\.[0-9]+)?$' THEN
                -- Standard format: 1234.56
                amount::DOUBLE PRECISION
            WHEN amount ~ '^[^0-9]*([0-9]+(\.[0-9]{3})*(,[0-9]+)?).*$' THEN
                -- Amount with currency symbols: €1.234,56 or 1,234.56 USD
                REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^0-9.,]', '', 'g'), '\.', '', 'g'), ',', '.')::DOUBLE PRECISION
            ELSE NULL
        END AS amount_parsed,
        currency,
        close_date,
        account_kundennummer
    FROM normalized
),

parsed_date AS (
    SELECT
        opp_kennung,
        name,
        stage,
        amount_parsed,
        currency,
        CASE
            WHEN close_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN close_date
            WHEN close_date ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$' THEN
                TO_CHAR(TO_DATE(close_date, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN close_date ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                TO_CHAR(TO_DATE(close_date, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN close_date ~ '^[0-9]{8}$' THEN
                TO_CHAR(TO_DATE(close_date, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS close_date_iso,
        account_kundennummer
    FROM parsed_amount
),

stage_mapped AS (
    SELECT
        opp_kennung,
        name,
        amount_parsed,
        currency,
        close_date_iso,
        account_kundennummer,
        CASE
            WHEN LOWER(stage) IN ('prospektierung', 'prospecting') THEN 'Prospecting'
            WHEN LOWER(stage) IN ('qualifikation', 'qualification') THEN 'Qualification'
            WHEN LOWER(stage) IN ('bedarfsanalyse', 'needs analysis') THEN 'Needs Analysis'
            WHEN LOWER(stage) IN ('wertangebot', 'value proposition') THEN 'Value Proposition'
            WHEN LOWER(stage) IN ('entscheidungsträger identifizieren', 'id. decision makers') THEN 'Id. Decision Makers'
            WHEN LOWER(stage) IN ('wahrnehmungsanalyse', 'perception analysis') THEN 'Perception Analysis'
            WHEN LOWER(stage) IN ('angebot/preisangebot', 'proposal/price quote') THEN 'Proposal/Price Quote'
            WHEN LOWER(stage) IN ('verhandlung/prüfung', 'negotiation/review') THEN 'Negotiation/Review'
            WHEN LOWER(stage) IN ('geschlossen gewonnen', 'closed won') THEN 'Closed Won'
            WHEN LOWER(stage) IN ('geschlossen verloren', 'closed lost') THEN 'Closed Lost'
            ELSE NULL
        END AS stage_name
    FROM parsed_date
)

SELECT
    MD5(opp_kennung || '_OPPORTUNITY') AS "Id",
    name AS "Name",
    stage_name AS "StageName",
    close_date_iso AS "CloseDate",
    amount_parsed AS "Amount",
    currency AS "CurrencyIsoCode",
    CASE
        WHEN account_kundennummer IS NOT NULL
        THEN MD5(account_kundennummer || '_ACCOUNT')
        ELSE NULL
    END AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM stage_mapped
WHERE stage_name IS NOT NULL
