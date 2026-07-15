{{ config(materialized='table') }}

SELECT
    -- Id: Strip letter prefixes from opp_kennung to match canonical Salesforce-style format
    REGEXP_REPLACE(TRIM(UPPER(opp_kennung)), '^[A-Z]+', '', 'g') AS "Id",

    -- Name: From titel with INITCAP normalization; fallback for empty/null
    COALESCE(NULLIF(TRIM(INITCAP(titel)), ''), 'Unnamed Opportunity') AS "Name",

    -- StageName: Map German/English source values to Salesforce standard stages
    CASE
        WHEN UPPER(TRIM(vertriebsphase)) IN ('PROSPECTING') THEN 'Prospecting'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('QUALIFICATION') THEN 'Qualification'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('BEDARFSANALYSE', 'NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('VALUE PROPOSITION', 'PROPOSITION WERT', 'MEHRWERT') THEN 'Value Proposition'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN', 'IDENTIFY DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('ANGEBOT PREISANGABE', 'PROPOSAL/PRICE QUOTE', 'ANGEBOOT', 'PREISEANGABE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('NEGOTIATION REVIEW', 'VERHANDLUNG', 'ÜBERPRÜFUNG', 'REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('ABSCHLUSS ERFOLGREICH', 'CLOSED WON', 'GEWONNEN') THEN 'Closed Won'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('ABSCHLUSS VERLOREN', 'CLOSED LOST', 'VERLOREN') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",

    -- CloseDate: Parse DD.MM.YYYY, YYYY-MM-DD, or YYYYMMDD; NULL on failure
    CASE
        WHEN zieldatum IS NOT NULL AND TRIM(zieldatum) != '' THEN
            CASE
                WHEN TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$'
                    THEN TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
                WHEN TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$'
                    THEN TO_DATE(TRIM(zieldatum), 'YYYY-MM-DD')::TEXT
                WHEN TRIM(zieldatum) ~ '^\d{8}$'
                    THEN TO_DATE(TRIM(zieldatum), 'YYYYMMDD')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "CloseDate",

    -- Amount: Strip currency codes/symbols, remove thousand-separator dots, swap comma to decimal point. Guard against empty result before casting.
    CASE
        WHEN auftragswert IS NOT NULL AND TRIM(auftragswert) != '' THEN
            CASE
                WHEN REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(TRIM(auftragswert), '[A-Za-z€$£]', '', 'g'), '.', ''), ',', '.') ~ '^-?\d+(\.\d+)?$'
                    THEN REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(TRIM(auftragswert), '[A-Za-z€$£]', '', 'g'), '.', ''), ',', '.')::DOUBLE PRECISION
                ELSE NULL
            END
        ELSE NULL
    END AS "Amount",

    -- CurrencyIsoCode: Trim and uppercase the currency code
    TRIM(UPPER(waehrungscode)) AS "CurrencyIsoCode",

    -- AccountId: Normalize kunden_ref using same prefix-stripping transform as Account.Id so it matches correctly
    REGEXP_REPLACE(TRIM(UPPER(kunden_ref)), '^[A-Z]+', '', 'g') AS "AccountId",

    -- Legacy_Opportunity_ID__c: Raw source key for row-level verification
    opp_kennung AS "Legacy_Opportunity_ID__c",

    -- CreatedDate/LastModifiedDate: Fixed timestamps from dbt run context
    CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS TEXT) AS "CreatedDate",
    CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS TEXT) AS "LastModifiedDate",

    -- IsDeleted: 0 (source has no soft-delete indicator)
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
WHERE opp_kennung IS NOT NULL AND TRIM(opp_kennung) != ''