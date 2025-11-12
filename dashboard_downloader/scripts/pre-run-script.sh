# 1) Ensure Poetry sees pyproject at the root
poetry env use python3.12

# 2) Install deps from pyproject.toml
poetry install

# 3) Install Playwright browsers (we’ll use system Chrome on macOS for the first-login)
poetry run python -m playwright install chromium
