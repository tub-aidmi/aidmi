{{ config(materialized='table') }}

SELECT
    'CON_' || kontakt_id AS "Id",
    rufname AS "FirstName",
    familienname AS "LastName",
    kontakt_email AS "Email",
    tel AS "Phone",
    berufsbezeichnung AS "Title",
    CASE 
        WHEN UPPER(rolle) IN ('ENTSCHEIDUNGSTRÄGER', 'ENTSCHEIDUNGSTRAGER', 'DECISION MAKER') THEN 'Decision Maker'
        WHEN UPPER(rolle) IN ('ENDANWENDER', 'END USER') THEN 'End User'
        WHEN UPPER(rolle) IN ('TECHNISCHER KONTAKT', 'TECHNICAL CONTACT') THEN 'Technical Contact'
        WHEN UPPER(rolle) IN ('EXEKUTIV', 'EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(korrespondenzsprache) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(korrespondenzsprache) IN ('EN', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(korrespondenzsprache) IN ('FR', 'FRANZÖSISCH', 'FRANZOSISCH') THEN 'FR'
        WHEN UPPER(korrespondenzsprache) IN ('ES', 'SPANISCH') THEN 'ES'
        WHEN UPPER(korrespondenzsprache) IN ('IT', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    'ACC_' || kd_nummer AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
