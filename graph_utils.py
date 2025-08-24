import requests

def graph_download_file_by_site_path(settings: dict, access_token: str) -> bytes | None:
    host = settings.get("hostname","").strip()
    site = settings.get("site_path","").strip()
    file_path = settings.get("file_path","").strip()
    url = f"https://graph.microsoft.com/v1.0/sites/{host}:{site}:/drive/root:{file_path}:/content"
    resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=60)
    return resp.content if resp.status_code == 200 else None

def graph_upload_file_by_site_path(settings: dict, access_token: str, content: bytes) -> bool:
    host = settings.get("hostname","").strip()
    site = settings.get("site_path","").strip()
    file_path = settings.get("file_path","").strip()
    url = f"https://graph.microsoft.com/v1.0/sites/{host}:{site}:/drive/root:{file_path}:/content"
    resp = requests.put(url, headers={"Authorization": f"Bearer {access_token}"}, data=content, timeout=60)
    return 200 <= resp.status_code < 300
