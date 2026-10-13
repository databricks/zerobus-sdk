# Source in bash/zsh: source purec-perf-bench/mint_token.sh catalog.schema.table
set +x
ZB_TABLE="${1:?Pass catalog.schema.table}"
ZB_WORKSPACE_URL="${ZB_WORKSPACE_URL:-https://e2-dogfood.staging.cloud.databricks.com}"
ZB_WORKSPACE_ID="${ZB_WORKSPACE_ID:-6051921418418893}"
unset ZB_TOKEN
ZB_AUTH_DETAILS="$(jq -cn --arg t "$ZB_TABLE" '[{type:"unity_catalog_privileges",privileges:["USE CATALOG"],object_type:"CATALOG",object_full_path:($t|split(".")[0])},{type:"unity_catalog_privileges",privileges:["USE SCHEMA"],object_type:"SCHEMA",object_full_path:($t|split(".")[:2]|join("."))},{type:"unity_catalog_privileges",privileges:["SELECT","MODIFY"],object_type:"TABLE",object_full_path:$t,operations:["zerobuswrite"]}]')" || return 1
ZB_TOKEN="$(curl -fsS --max-time 30 --user "${DATABRICKS_CLIENT_ID:?SP client ID missing}:${DATABRICKS_CLIENT_SECRET:?SP secret missing}" --data 'grant_type=client_credentials' --data 'scope=all-apis' --data-urlencode "resource=api://databricks/workspaces/$ZB_WORKSPACE_ID/zerobusDirectWriteApi" --data-urlencode "authorization_details=$ZB_AUTH_DETAILS" "$ZB_WORKSPACE_URL/oidc/v1/token" | jq -er '.access_token | select(type == "string" and length > 0)')" || return 1
export ZB_TOKEN
