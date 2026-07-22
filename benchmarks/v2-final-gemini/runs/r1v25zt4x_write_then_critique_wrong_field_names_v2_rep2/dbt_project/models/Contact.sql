{{ config(materialized='table') }}

SELECT
    MD5(ansprechpartner.ap_id) AS "Id",
    ansprechpartner.vorname AS "FirstName",
    ansprechpartner.nachname AS "LastName",
    ansprechpartner.email_adresse AS "Email",
    ansprechpartner.telefonnummer AS "Phone",
    INITCAP(TRIM(ansprechpartner.position)) AS "Title",
    CASE
        WHEN ansprechpartner.funktion IN ('Decision Maker', 'End User', 'Technical Contact', 'Executive Sponsor')
        THEN ansprechpartner.funktion
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN ansprechpartner.sprache IN ('DE', 'EN', 'FR', 'ES', 'IT')
        THEN ansprechpartner.sprache
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(ansprechpartner.kunde) AS "AccountId",
    ansprechpartner.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ansprechpartner
WHERE
    ansprechpartner.nachname IS NOT NULL