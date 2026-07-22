{{ config(materialized='table') }}

SELECT
    MD5(ansprechpartner.ap_id) AS "Id",
    ansprechpartner.vorname AS "FirstName",
    COALESCE(ansprechpartner.nachname, 'Unknown') AS "LastName",
    ansprechpartner.email_adresse AS "Email",
    ansprechpartner.telefonnummer AS "Phone",
    ansprechpartner.position AS "Title",
    CASE
        WHEN LOWER(ansprechpartner.funktion) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(ansprechpartner.funktion) = 'end user' THEN 'End User'
        WHEN LOWER(ansprechpartner.funktion) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(ansprechpartner.funktion) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(ansprechpartner.sprache) = 'de' THEN 'DE'
        WHEN LOWER(ansprechpartner.sprache) = 'en' THEN 'EN'
        WHEN LOWER(ansprechpartner.sprache) = 'fr' THEN 'FR'
        WHEN LOWER(ansprechpartner.sprache) = 'es' THEN 'ES'
        WHEN LOWER(ansprechpartner.sprache) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(ansprechpartner.kunde) AS "AccountId",
    ansprechpartner.ap_id AS "Legacy_Contact_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ansprechpartner
