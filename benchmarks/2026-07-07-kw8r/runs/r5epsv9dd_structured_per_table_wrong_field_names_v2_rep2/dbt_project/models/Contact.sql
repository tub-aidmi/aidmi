{{ config(materialized='table') }}

SELECT
    ap_id AS "Id",
    TRIM(vorname) AS "FirstName",
    TRIM(nachname) AS "LastName",
    TRIM(email_adresse) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    TRIM(position) AS "Title",
    CASE
        WHEN UPPER(TRIM(funktion)) LIKE '%ENTSCHEIDUNG%' THEN 'Decision Maker'
        WHEN UPPER(TRIM(funktion)) LIKE '%TECHNISCH%' THEN 'Technical Contact'
        WHEN UPPER(TRIM(funktion)) LIKE '%ANWENDER%' THEN 'End User'
        WHEN UPPER(TRIM(funktion)) LIKE '%GESCHÄFTSFÜHRUNG%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(sprache)) LIKE '%DEUTSCH%' THEN 'DE'
        WHEN UPPER(TRIM(sprache)) LIKE '%ENGLISCH%' THEN 'EN'
        WHEN UPPER(TRIM(sprache)) LIKE '%FRANZÖSISCH%' THEN 'FR'
        WHEN UPPER(TRIM(sprache)) LIKE '%SPANISCH%' THEN 'ES'
        WHEN UPPER(TRIM(sprache)) LIKE '%ITALIENISCH%' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    k.kunden_nr AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON ap.kunde = k.kunden_nr