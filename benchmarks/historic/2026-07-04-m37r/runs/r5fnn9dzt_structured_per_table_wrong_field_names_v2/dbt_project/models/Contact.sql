{{ config(materialized='table') }}

SELECT
    t1.ap_id AS "Id",
    TRIM(t1.vorname) AS "FirstName",
    COALESCE(TRIM(t1.nachname), 'Unknown') AS "LastName",
    TRIM(t1.email_adresse) AS "Email",
    TRIM(t1.telefonnummer) AS "Phone",
    TRIM(t1.position) AS "Title",
    t1.funktion AS "Role__c",
    t1.sprache AS "Preferred_Language__c",
    MD5(TRIM(t1.kunde)) AS "AccountId",
    t1.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS t1
