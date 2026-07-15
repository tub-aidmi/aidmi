{{ config(materialized='table') }}
SELECT
    ap."ap_id" AS "Id",
    TRIM(ap."vorname") AS "FirstName",
    TRIM(ap."nachname") AS "LastName",
    TRIM(ap."email_adresse") AS "Email",
    TRIM(ap."telefonnummer") AS "Phone",
    TRIM(ap."position") AS "Title",
    CASE
        WHEN TRIM(ap."funktion") ILIKE '%decision maker%' THEN 'Decision Maker'
        WHEN TRIM(ap."funktion") ILIKE '%end user%' THEN 'End User'
        WHEN TRIM(ap."funktion") ILIKE '%technical contact%' THEN 'Technical Contact'
        WHEN TRIM(ap."funktion") ILIKE '%executive sponsor%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(ap."sprache") IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(ap."sprache"))
        ELSE NULL
    END AS "Preferred_Language__c",
    LPAD('001' || REGEXP_REPLACE(k."kunden_nr", '-', ''), 18, '0') AS "AccountId",
    ap."ap_id" AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON k."kunden_nr" = ap."kunde"