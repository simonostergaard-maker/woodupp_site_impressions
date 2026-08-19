@echo off
setlocal

:: ============================================================
::  WoodUpp Dashboard — Daily Update Script
::  1. Pull latest code from GitHub
::  2. Extract GSC data from BigQuery  -> data/woodupp_url_impressions.csv
::  3. Rebuild index.html              (preprocess.py)
::  4. Commit and push to GitHub
:: ============================================================

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\sos\Desktop\Claude\Woodupp\obsidian-375910-74200778fd7c.json

echo.
echo ============================================================
echo  WoodUpp Dashboard Update — %DATE% %TIME%
echo ============================================================

:: ── Step 1: Pull latest code ─────────────────────────────────
echo.
echo [1/4] Pulling latest code from GitHub...
git pull origin main
if errorlevel 1 (
    echo ERROR: git pull failed. Aborting.
    pause & exit /b 1
)

:: ── Step 2: Extract GSC data from BigQuery ───────────────────
echo.
echo [2/4] Extracting GSC data from BigQuery...
python extract_gsc.py
if errorlevel 1 (
    echo ERROR: extract_gsc.py failed. Aborting.
    pause & exit /b 1
)

:: ── Step 3: Rebuild the dashboard HTML ───────────────────────
echo.
echo [3/4] Rebuilding dashboard...
python preprocess.py data\woodupp_url_impressions.csv
if errorlevel 1 (
    echo ERROR: preprocess.py failed. Aborting.
    pause & exit /b 1
)

:: ── Step 4: Commit and push to GitHub ────────────────────────
echo.
echo [4/4] Pushing to GitHub...
git add index.html
git diff --cached --quiet
if errorlevel 1 (
    git commit -m "Dashboard update %DATE%"
    git pull --rebase origin main
    git push origin main
    if errorlevel 1 (
        echo ERROR: git push failed.
        pause & exit /b 1
    )
    echo.
    echo Done! Live dashboard will refresh in ~1 minute.
    echo https://simonostergaard-maker.github.io/woodupp_site_impressions/
) else (
    echo No changes to commit — dashboard is already up to date.
)

echo.
echo ============================================================
echo  Finished — %TIME%
echo ============================================================
pause
