{{ config(materialized='table') }}

SELECT
    CAST(ap_id AS text) AS "Id",
    INITCAP(TRIM(vorname)) AS "FirstName",
    COALESCE(NULLIF(INITCAP(TRIM(nachname)), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(email_adresse)) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    INITCAP(TRIM(position)) AS "Title",
    CASE UPPER(TRIM(funktion))
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'ENDANWENDER' THEN 'End User'
        WHEN 'TECHNISCHER ANSPRECHPARTNER' THEN 'Technical Contact'
        WHEN 'VORSTAND' THEN 'Executive Sponsor'
        WHEN 'GESCHÄFTSFÜHRER' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(sprache))
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
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || TRIM(kunde) AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}