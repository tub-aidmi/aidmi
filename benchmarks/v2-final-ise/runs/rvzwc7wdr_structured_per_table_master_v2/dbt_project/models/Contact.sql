{{ config(materialized='table') }}

WITH contacts AS (
    SELECT
        -- Generate Salesforce-style 18-char Id
        '003' || LPAD(REGEXP_REPLACE(k.kontakt_id, '[^0-9]', ''), 15, '0') AS "Id",
        
        -- Map source fields
        TRIM(rufname) AS "FirstName",
        TRIM(familienname) AS "LastName",
        kontakt_email AS "Email",
        tel AS "Phone",
        berufsbezeichnung AS "Title",
        
        -- Role mapping to enum values
        CASE LOWER(TRIM(k.rolle))
            WHEN 'technical contact' THEN 'Technical Contact'
            WHEN 'technischer ansprechpartner' THEN 'Technical Contact'
            WHEN 'techniker' THEN 'Technical Contact'
            WHEN 'end user' THEN 'End User'
            WHEN 'endanwender' THEN 'End User'
            WHEN 'decision maker' THEN 'Decision Maker'
            WHEN 'entscheider' THEN 'Decision Maker'
            WHEN 'executive sponsor' THEN 'Executive Sponsor'
            WHEN 'sponsor' THEN 'Executive Sponsor'
            ELSE NULL
        END AS "Role__c",
        
        -- Preferred language mapping to ISO codes
        CASE UPPER(TRIM(k.korrespondenzsprache))
            WHEN 'DE' THEN 'DE'
            WHEN 'EN' THEN 'EN'
            WHEN 'FR' THEN 'FR'
            WHEN 'ES' THEN 'ES'
            WHEN 'IT' THEN 'IT'
            WHEN 'GERMAN' THEN 'DE'
            WHEN 'ENGLISH' THEN 'EN'
            WHEN 'FRENCH' THEN 'FR'
            WHEN 'SPANISH' THEN 'ES'
            WHEN 'ITALIAN' THEN 'IT'
            WHEN 'DEUTSCH' THEN 'DE'
            WHEN 'ENGLISCH' THEN 'EN'
            WHEN 'FRANZÖSISCH' THEN 'FR'
            ELSE NULL
        END AS "Preferred_Language__c",
        
        -- AccountId: Salesforce-style Id derived from customer number
        CASE 
            WHEN mk.kundennummer IS NOT NULL 
            THEN '001' || LPAD(REGEXP_REPLACE(mk.kundennummer, '[^0-9]', ''), 15, '0')
            ELSE NULL
        END AS "AccountId",
        
        -- Legacy contact ID (source natural key)
        k.kontakt_id AS "Legacy_Contact_ID__c",
        
        -- Dates and flags (not available in source; use defaults)
        NULL AS "CreatedDate",
        NULL AS "LastModifiedDate",
        0 AS "IsDeleted"
        
    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
        ON k.kd_nummer = mk.kundennummer
)

SELECT * FROM contacts