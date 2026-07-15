{{ config(materialized='table') }}

SELECT 
    -- Id: Generated using consistent prefix convention for traceability
    'OPP-' || TRIM(opp_kennung) AS "Id",

    -- Name: Title with INITCAP and TRIM normalization; fallback to empty string if NULL
    INITCAP(TRIM(titel)) AS "Name",

    -- StageName: Map German vertriebsphase values to English enum domain with fallback to NULL
    CASE 
        WHEN TRIM(vertriebsphase) = 'Anfrage' THEN 'Prospecting'
        WHEN TRIM(vertriebsphase) = 'Qualifikation' THEN 'Qualification'
        WHEN TRIM(vertriebsphase) = 'Bedarfsanalyse' THEN 'Needs Analysis'
        WHEN TRIM(vertriebsphase) = 'Wertversprechen' THEN 'Value Proposition'
        WHEN TRIM(vertriebsphase) = 'Entscheidungsträger identifizieren' THEN 'Id. Decision Makers'
        WHEN TRIM(vertriebsphase) = 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN TRIM(vertriebsphase) = 'Angebot/Preisangebot' THEN 'Proposal/Price Quote'
        WHEN TRIM(vertriebsphase) = 'Verhandlung/Überprüfung' THEN 'Negotiation/Review'
        WHEN TRIM(vertriebsphase) = 'Fertig' THEN 'Closed Won'
        WHEN TRIM(vertriebsphase) = 'Verloren' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",

    -- CloseDate: Parse source dates to ISO YYYY-MM-DD format; prefer NULL over sentinel dates
    CASE 
        WHEN zieldatum IS NOT NULL AND TRIM(zieldatum) != '' THEN 
            CASE 
                WHEN TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
                    TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
                WHEN TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN 
                    TRIM(zieldatum)
                WHEN TRIM(zieldatum) ~ '^\d{8}$' THEN 
                    LEFT(TRIM(zieldatum), 4) || '-' || SUBSTR(TRIM(zieldatum), 5, 2) || '-' || SUBSTR(TRIM(zieldatum), 7, 2)
                ELSE NULL
            END
        ELSE NULL
    END AS "CloseDate",

    -- Amount: Strip currency symbols/text, handle European locale formatting (dot=thousands, comma=decimal)
    -- Guard against values that contain no digits after cleanup (e.g. pure currency symbols or whitespace)
    CASE 
        WHEN auftragswert IS NOT NULL AND TRIM(auftragswert) != '' AND TRIM(auftragswert) ~ '[\d]' THEN
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(auftragswert), '[^\d.,]', '', 'g'),
                    '\.', ''),       -- Remove thousand-separator dots
                ',', '.')           -- Convert decimal comma to dot
            AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",

    -- CurrencyIsoCode: Standardize source currency code to uppercase ISO format
    UPPER(TRIM(waehrungscode)) AS "CurrencyIsoCode",

    -- AccountId: Resolve via master_kunden join; use Salesforce-style 'CUST-' prefix for consistency with Account.Id
    'CUST-' || TRIM(k.kundennummer) AS "AccountId",

    -- Legacy_Opportunity_ID__c: Raw source natural key for row-level verification
    opp_kennung AS "Legacy_Opportunity_ID__c",

    -- CreatedDate: Default to current timestamp as text
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",

    -- LastModifiedDate: Default to current timestamp as text
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",

    -- IsDeleted: Literal 0 (soft delete flag, not used)
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o

-- Join with master_kunden to resolve AccountId via kunden_ref -> kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON TRIM(o.kunden_ref) = TRIM(k.kundennummer)