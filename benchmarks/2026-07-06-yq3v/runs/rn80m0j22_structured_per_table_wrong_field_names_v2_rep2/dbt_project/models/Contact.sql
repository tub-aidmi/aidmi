-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}

{{ config(materialized='table') }}

SELECT
    TRIM(ap_id) AS "Id",
    TRIM(vorname) AS "FirstName",
    COALESCE(TRIM(nachname), 'Unknown') AS "LastName",
    TRIM(email_adresse) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    TRIM(position) AS "Title",
    CASE
        WHEN TRIM(funktion) = 'Decision Maker' THEN 'Decision Maker'
        WHEN TRIM(funktion) = 'End User' THEN 'End User'
        WHEN TRIM(funktion) = 'Technical Contact' THEN 'Technical Contact'
        WHEN TRIM(funktion) = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(sprache) = 'DE' THEN 'DE'
        WHEN TRIM(sprache) = 'EN' THEN 'EN'
        WHEN TRIM(sprache) = 'FR' THEN 'FR'
        WHEN TRIM(sprache) = 'ES' THEN 'ES'
        WHEN TRIM(sprache) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(kunde) AS "AccountId",
    TRIM(ap_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}