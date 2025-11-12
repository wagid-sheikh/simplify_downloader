# File: downloader/page_selectors.py
LOGIN_USERNAME = "input[name='user_name']"
LOGIN_PASSWORD = "input[name='password']"
LOGIN_SUBMIT   = "button[type='submit']"

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