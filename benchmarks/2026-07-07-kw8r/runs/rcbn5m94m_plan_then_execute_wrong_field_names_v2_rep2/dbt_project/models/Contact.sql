{{ config(materialized='table') }}

SELECT
    '701' || TRIM(ap.ap_id) AS "Id",
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    INITCAP(COALESCE(NULLIF(TRIM(ap.nachname), ''), 'Unknown')) AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    REGEXP_REPLACE(TRIM(ap.telefonnummer), '[^0-9+]', '', 'g') AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",
    CASE LOWER(TRIM(ap.funktion))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'buyer' THEN 'Decision Maker'
        WHEN 'owner' THEN 'Decision Maker'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'end user' THEN 'End User'
        WHEN 'exec sponsor' THEN 'Executive Sponsor'
        WHEN 'champion' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(ap.sprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || TRIM(k.kunden_nr) AS "AccountId",
    TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP()::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP()::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(ap.kunde) = TRIM(k.kunden_nr)