{{ config(materialized='table') }}

SELECT 
    -- Transform opp_kennung into Salesforce-style Opportunity Id (006 prefix)
    CONCAT('006', LPAD(opp_kennung, 15, '0')) AS "Id",
    
    -- Name from titel (German for title)
    COALESCE(INITCAP(TRIM(titel)), 'Unknown Opportunity') AS "Name",
    
    -- StageName: Map German/English vertriebsphase to Salesforce pipeline stages
    CASE LOWER(TRIM(vertriebsphase))
        WHEN 'akquise' THEN 'Prospecting'
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'wertproposition' THEN 'Value Proposition'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'angebot/preisanfrage' THEN 'Proposal/Price Quote'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung/überprüfung' THEN 'Negotiation/Review'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'  -- fallback for unmapped or null values
    END AS "StageName",
    
    -- CloseDate: Parse from multiple European formats (DD.MM.YYYY, YYYYMMDD) into ISO format
    CASE 
        WHEN zieldatum IS NULL OR TRIM(zieldatum) = '' THEN NULL
        WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
            TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
        WHEN zieldatum ~ '^\d{8}$' THEN 
            SUBSTR(TRIM(zieldatum), 1, 4) || '-' || 
            SUBSTR(TRIM(zieldatum), 5, 2) || '-' || 
            SUBSTR(TRIM(zieldatum), 7, 2)
        WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(zieldatum)
        ELSE NULL
    END AS "CloseDate",
    
    -- Amount: Strip currency symbols/text, handle European decimal format (dots=thousands, comma=decimal)
    CASE 
        WHEN auftragswert IS NULL OR TRIM(auftragswert) = '' THEN NULL
        ELSE 
            REGEXP_REPLACE(
                -- Normalize to standard decimal notation
                CASE 
                    -- Detect European format: comma with 1-3 digits at end
                    WHEN REGEXP_REPLACE(TRIM(auftragswert), '[^\d.,]', '', 'g') ~ ',\d{1,3}$' THEN
                        REPLACE(
                            REPLACE(
                                TRIM(REGEXP_REPLACE(auftragswert, '[A-Z€$£\s]', '')),
                                '.', ''  -- Remove thousand-separator dots
                            ),
                            ',', '.'  -- Convert decimal comma to period
                        )
                    ELSE
                        REGEXP_REPLACE(TRIM(auftragswert), '[A-Z€$£\s]', '', 'gi')
                END
            , '\.$', '')::DOUBLE PRECISION
    END AS "Amount",
    
    -- CurrencyIsoCode: Normalize to ISO 4217 standard uppercase codes
    UPPER(TRIM(waehrungscode)) AS "CurrencyIsoCode",
    
    -- AccountId: Transform kunden_ref into Salesforce-style Account Id (001 prefix)
    CONCAT('001', LPAD(kunden_ref, 15, '0')) AS "AccountId",
    
    -- Legacy_Opportunity_ID__c: Preserve the source natural key for audit/traceability
    opp_kennung AS "Legacy_Opportunity_ID__c",
    
    -- CreatedDate and LastModifiedDate: Not available in source system
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    
    -- IsDeleted: Default to false (0)
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}