{{ config(materialized='table') }}

SELECT
    UPPER(SUBSTRING(MD5(LOWER(ap_id::text)) FROM 1 FOR 18)) AS "Id",
    INITCAP(TRIM(vorname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(nachname)), 'Unknown') AS "LastName",
    LOWER(TRIM(email_adresse)) AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE LOWER(TRIM(COALESCE(funktion, '')))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        -- German role values
        WHEN 'entscheidungstraeger' THEN 'Decision Maker'
        WHEN 'benutzer' THEN 'End User'
        WHEN 'technischer ansprechpartner' THEN 'Technical Contact'
        WHEN 'sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(COALESCE(sprache, '')))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        -- German language names to ISO codes
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'ENGELISCH' THEN 'EN'
        WHEN 'FRENZESISCH' THEN 'FR'
        WHEN 'SPANISCH' THEN 'ES'
        WHEN 'ITALIENISCH' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    UPPER(SUBSTRING(MD5(LOWER(kunde::text)) FROM 1 FOR 18)) AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
