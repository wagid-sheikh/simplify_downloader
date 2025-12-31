# File: dashboard_downloader/page_selectors.py
LOGIN_USERNAME = "#txtUserId"
LOGIN_PASSWORD = "#txtPassword"
LOGIN_STORE_CODE = "#txtBranchPin"
LOGIN_SUBMIT = "#btnLogin, button:has-text('Login')"

# Dashboard download links (hypothetical)
# Match any <a> link that triggers a download.
# DOWNLOAD_LINKS = "a[title='Download'], a[href*='download']"

DOWNLOAD_LINKS = (
    "a[title='Download'], "
    "a[href*='download'], "
    "a[href*='export'], "
    "a[download], "
    "a:has-text('Download'), "
    "button:has-text('Download'), "
    "button[title='Download']"
)
