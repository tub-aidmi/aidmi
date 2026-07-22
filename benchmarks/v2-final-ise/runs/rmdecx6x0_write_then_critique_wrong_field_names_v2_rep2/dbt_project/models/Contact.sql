{{ config(materialized='table') }}

SELECT
    CAST(ap."ap_id" AS TEXT) AS "Id",
    ap."vorname" AS "FirstName",
    COALESCE(ap."nachname", 'Unknown') AS "LastName",
    ap."email_adresse" AS "Email",
    ap."telefonnummer" AS "Phone",
    ap."position" AS "Title",
    CASE
        WHEN LOWER(TRIM(ap."funktion")) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(ap."funktion")) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(ap."funktion")) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(ap."funktion")) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN ap."sprache" IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN ap."sprache"
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || SUBSTRING(MD5(ap.kunde), 1, 15) AS "AccountId",
    CAST(ap."ap_id" AS TEXT) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap