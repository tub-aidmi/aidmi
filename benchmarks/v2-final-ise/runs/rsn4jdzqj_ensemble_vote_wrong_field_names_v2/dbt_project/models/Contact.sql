{{ config(materialized='table') }}

SELECT
    ap."ap_id" AS "Id",
    TRIM(ap."vorname") AS "FirstName",
    TRIM(ap."nachname") AS "LastName",
    TRIM(ap."email_adresse") AS "Email",
    TRIM(ap."telefonnummer") AS "Phone",
    TRIM(ap."position") AS "Title",
    CASE
        WHEN UPPER(TRIM(ap."funktion")) LIKE '%ENTSCHEIDER%' THEN 'Decision Maker'
        WHEN UPPER(TRIM(ap."funktion")) LIKE '%ENDNUTZER%' THEN 'End User'
        WHEN UPPER(TRIM(ap."funktion")) LIKE '%TECHNIK%' THEN 'Technical Contact'
        WHEN UPPER(TRIM(ap."funktion")) LIKE '%GESCHÄFTSFÜHRER%' OR UPPER(TRIM(ap."funktion")) LIKE '%SPONSOR%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(ap."sprache")) LIKE '%DEUTSCH%' THEN 'DE'
        WHEN UPPER(TRIM(ap."sprache")) LIKE '%ENGLISCH%' THEN 'EN'
        WHEN UPPER(TRIM(ap."sprache")) LIKE '%FRANZÖSISCH%' THEN 'FR'
        WHEN UPPER(TRIM(ap."sprache")) LIKE '%SPANISCH%' THEN 'ES'
        WHEN UPPER(TRIM(ap."sprache")) LIKE '%ITALIENISCH%' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    'acc_' || k."kunden_nr" AS "AccountId",
    ap."ap_id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(ap."kunde") = TRIM(k."kunden_nr")