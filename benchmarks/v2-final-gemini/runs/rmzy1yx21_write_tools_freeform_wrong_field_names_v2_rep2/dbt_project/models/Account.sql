-- models/account.sql

{{ config(materialized='table') }}

SELECT
    id AS "AccountId",
    name AS "AccountName",
    'Legacy' AS "Legacy_Account__c"
FROM {{ source('salesforce', 'account') }}
