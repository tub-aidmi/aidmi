{{ config(materialized='table') }}

SELECT
    '003' || SUBSTRING(MD5(ap_id) FROM 1 FOR 14) AS "Id",
    INITCAP(TRIM(vorname)) AS "FirstName",
    COALESCE(NULLIF(TRIM(nachname), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(email_adresse)) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    INITCAP(TRIM(position)) AS "Title",
    CASE funktion
        WHEN 'Decision Maker' THEN 'Decision Maker'
        WHEN 'End User' THEN 'End User'
        WHEN 'Technical Contact' THEN 'Technical Contact'
        WHEN 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE sprache
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || SUBSTRING(MD5(kunde) FROM 1 FOR 14) AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}