import msal, requests

AUTH_SCOPES = [
    "User.Read", "offline_access",
    "GroupMember.Read.All", "Sites.ReadWrite.All", "Files.ReadWrite.All",
]

def get_msal_app(tenant_id: str, client_id: str, client_secret: str):
    return msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret,
    )

def get_access_token(tenant_id, client_id, client_secret):
    app = get_msal_app(tenant_id, client_id, client_secret)
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    return result.get("access_token")

def get_authorization_url(tenant_id, client_id, client_secret, redirect_uri, scopes=None):
    app = get_msal_app(tenant_id, client_id, client_secret)
    return app.get_authorization_request_url(
        scopes or AUTH_SCOPES, redirect_uri=redirect_uri
    )

def acquire_token_by_auth_code(tenant_id, client_id, client_secret, auth_code, redirect_uri, scopes=None):
    app = get_msal_app(tenant_id, client_id, client_secret)
    return app.acquire_token_by_authorization_code(
        auth_code, scopes=scopes or AUTH_SCOPES, redirect_uri=redirect_uri
    )

def user_context(access_token: str, admin_group: str = "MES-Admin", operator_group: str = "MES-Operador"):
    headers = {"Authorization": f"Bearer {access_token}"}
    def graph_get(url):
        r = requests.get(url, headers=headers, timeout=60); r.raise_for_status(); return r.json()
    me = graph_get("https://graph.microsoft.com/v1.0/me")
    groups = graph_get("https://graph.microsoft.com/v1.0/me/memberOf?$select=displayName")
    group_names = [g.get("displayName", "") for g in groups.get("value", [])]
    rol = "admin" if admin_group in group_names else ("operador" if operator_group in group_names else "operador")
    return {"upn": me.get("userPrincipalName", ""), "rol": rol, "groups": group_names}
