{{ config(materialized='table') }}

SELECT
    ap_id AS "Id",
    vorname AS "FirstName",
    COALESCE(NULLIF(TRIM(nachname), ''), 'Unknown') AS "LastName",
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE
        WHEN UPPER(funktion) LIKE '%ENTSCHEIDER%' THEN 'Decision Maker'
        WHEN UPPER(funktion) LIKE '%END NUTZER%' THEN 'End User'
        WHEN UPPER(funktion) LIKE '%TECHNIK%' THEN 'Technical Contact'
        WHEN UPPER(funktion) LIKE '%GESCHÄFTSFÜHRER%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(sprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(sprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(sprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(sprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(sprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    k.kunden_nr AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kunde = k.kunden_nr
