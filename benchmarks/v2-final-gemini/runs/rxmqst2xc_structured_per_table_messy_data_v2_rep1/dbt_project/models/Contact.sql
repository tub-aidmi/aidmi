-- noinspection SqlNoDataSourceInspection
-- noinspection SqlDialectInspection

{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    c.firstname AS "FirstName",
    COALESCE(c.lastname, 'Unknown') AS "LastName",
    c.email AS "Email",
    c.phone AS "Phone",
    c.title AS "Title",
    CASE
        WHEN UPPER(TRIM(c.role__c)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(c.role__c)) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(c.role__c)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(c.role__c)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(c.preferred_language__c)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(c.preferred_language__c)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(c.preferred_language__c)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(c.preferred_language__c)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(c.preferred_language__c)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    c.accountid AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS c