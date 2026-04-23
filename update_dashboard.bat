@echo off
setlocal

:: ─── WoodUpp Dashboard Updater ───────────────────────────────────────────────
:: Run this script whenever you have fresh GSC data.
:: It regenerates index.html and pushes to GitHub so the live dashboard updates.
:: ─────────────────────────────────────────────────────────────────────────────

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

echo.
echo [1/3] Generating dashboard from latest data...
python preprocess.py
if errorlevel 1 (
    echo ERROR: preprocess.py failed. Check output above.
    pause
    exit /b 1
)

echo.
echo [2/3] Committing updated dashboard...
git add index.html
git diff --cached --quiet
if errorlevel 1 (
    git commit -m "Update dashboard data %DATE%"
) else (
    echo No changes detected in index.html — nothing to commit.
    pause
    exit /b 0
)

echo.
echo [3/3] Pushing to GitHub...
git push origin main
if errorlevel 1 (
    echo ERROR: Push failed. Check your git credentials.
    pause
    exit /b 1
)

echo.
echo Done! The live dashboard will refresh in ~1 minute.
echo URL: https://simonostergaard-maker.github.io/woodupp_site_impressions/
echo.
pause
