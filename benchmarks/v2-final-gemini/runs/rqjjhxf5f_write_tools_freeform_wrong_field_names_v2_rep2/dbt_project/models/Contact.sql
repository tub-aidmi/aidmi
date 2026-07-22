{{ config(materialized='table') }}

SELECT
    MD5(ansprechpartner.ap_id) AS "Id",
    ansprechpartner.vorname AS "FirstName",
    COALESCE(ansprechpartner.nachname, 'Unknown') AS "LastName", -- LastName is NOT NULL
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
        WHEN UPPER(ansprechpartner.sprache) = 'DE' THEN 'DE'
        WHEN UPPER(ansprechpartner.sprache) = 'EN' THEN 'EN'
        WHEN UPPER(ansprechpartner.sprache) = 'FR' THEN 'FR'
        WHEN UPPER(ansprechpartner.sprache) = 'ES' THEN 'ES'
        WHEN UPPER(ansprechpartner.sprache) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(ansprechpartner.kunde) AS "AccountId", -- Links to kunden.kunden_nr
    ansprechpartner.ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ansprechpartner
