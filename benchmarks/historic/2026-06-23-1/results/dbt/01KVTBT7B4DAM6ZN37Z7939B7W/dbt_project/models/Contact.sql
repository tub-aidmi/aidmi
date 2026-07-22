{{ config(materialized='table') }}

SELECT
    kontakt_id AS Id,
    rufname AS FirstName,
    familienname AS LastName,
    kontakt_email AS Email,
    tel AS Phone,
    berufsbezeichnung AS Title,
    CASE 
        WHEN rolle IN ('Decision Maker', 'End User', 'Technical Contact', 'Executive Sponsor') THEN rolle
        ELSE 'End User'
    END AS Role__c,
    CASE 
        WHEN korrespondenzsprache = 'Deutsch' THEN 'DE'
        WHEN korrespondenzsprache = 'EN' THEN 'EN'
        WHEN korrespondenzsprache = 'FR' THEN 'FR'
        WHEN korrespondenzsprache = 'Español' THEN 'ES'
        WHEN korrespondenzsprache = 'Italiano' THEN 'IT'
        ELSE 'EN'
    END AS Preferred_Language__c,
    kd_nummer AS AccountId,
    kontakt_id AS Legacy_Contact_ID__c,
    CURRENT_TIMESTAMP::text AS CreatedDate,
    CURRENT_TIMESTAMP::text AS LastModifiedDate,
    0 AS IsDeleted

FROM {{ source('fixture_master_src', 'master_kontakte') }}
