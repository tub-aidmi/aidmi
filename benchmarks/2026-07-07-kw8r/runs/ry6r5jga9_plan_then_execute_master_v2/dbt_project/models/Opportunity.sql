{{ config(materialized='table') }}

SELECT 
    -- Id: Generated using consistent prefix convention for traceability
    'OPP-' || opp_kennung AS "Id",
    
    -- Name: Title with INITCAP and TRIM normalization
    INITCAP(TRIM(titel)) AS "Name",
    
    -- StageName: Map German vertriebsphase values to English enum domain with fallback
    CASE 
        WHEN vertriebsphase = 'Anfrage' THEN 'Prospecting'
        WHEN vertriebsphase = 'Qualifikation' THEN 'Qualification'
        WHEN vertriebsphase = 'Bedarfsanalyse' THEN 'Needs Analysis'
        WHEN vertriebsphase = 'Wertversprechen' THEN 'Value Proposition'
        WHEN vertriebsphase = 'Entscheidungsträger identifizieren' THEN 'Id. Decision Makers'
        WHEN vertriebsphase = 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN vertriebsphase = 'Angebot/Preisangebot' THEN 'Proposal/Price Quote'
        WHEN vertriebsphase = 'Verhandlung/Überprüfung' THEN 'Negotiation/Review'
        WHEN vertriebsphase = 'Fertig' THEN 'Closed Won'
        WHEN vertriebsphase = 'Verloren' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    
    -- CloseDate: Parse source dates to ISO YYYY-MM-DD format
    CASE 
        WHEN zieldatum IS NOT NULL AND zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
            TO_DATE(zieldatum, 'DD.MM.YYYY')::TEXT
        WHEN zieldatum IS NOT NULL AND zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN 
            zieldatum
        WHEN zieldatum IS NOT NULL AND zieldatum ~ '^\d{8}$' THEN 
            LEFT(zieldatum, 4) || '-' || SUBSTR(zieldatum, 5, 2) || '-' || SUBSTR(zieldatum, 7, 2)
        ELSE NULL
    END AS "CloseDate",
    
    -- Amount: Strip currency symbols/text, handle European locale formatting
    CASE 
        WHEN REGEXP_REPLACE(auftragswert, '[^0-9,.]', '', 'g') IS NOT NULL THEN
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(auftragswert), '[^\d.,]', '', 'g'),
                        '\.', ''),           -- Remove thousand-separator dots
                    ',', '.')              -- Convert decimal comma to dot
            ::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    
    -- CurrencyIsoCode: Standardize source currency code to uppercase ISO format
    UPPER(TRIM(waehrungscode)) AS "CurrencyIsoCode",
    
    -- AccountId: Join with master_kunden and use transformed Account Id ('CUST-' || kundennummer)
    'CUST-' || k.kundennummer AS "AccountId",
    
    -- Legacy_Opportunity_ID__c: Raw source natural key for row-level verification
    opp_kennung AS "Legacy_Opportunity_ID__c",
    
    -- CreatedDate: Default to current timestamp
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    
    -- LastModifiedDate: Default to current timestamp
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",
    
    -- IsDeleted: Literal 0 (soft delete flag, not used)
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o

-- Join with master_kunden to resolve AccountId via kunden_ref -> kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON o.kunden_ref = k.kundennummer