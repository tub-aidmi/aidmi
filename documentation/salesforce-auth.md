# Salesforce authentication (fixture `sf_pipedrive`)

SOAP login via [`simple-salesforce`](https://github.com/simple-salesforce/simple-salesforce) to `login.salesforce.com` (no `SF_DOMAIN` knob).

## Environment variables

Set **`SF_USERNAME`**, **`SF_PASSWORD`**, and **`SF_SECURITY_TOKEN`** in `.env` (see [.env.example](../.env.example)). Quote values that contain **`#`**, **`!`**, **`$`**, or spaces — e.g. `SF_PASSWORD="p#ss!word"`.

`aidmi-orchestrator` and `make sf-auth-check` load `.env` from the repo root; file values take precedence over shell exports.

## Debugging failed logins

1. Reset the user's **security token** in Salesforce Setup and retry.
2. Confirm the user's **profile** permits **API Enabled** / **SOAP login**.
3. Check org policies blocking password-based API login (common with SSO-only users).
