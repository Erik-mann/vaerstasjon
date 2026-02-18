@echo off
setlocal enabledelayedexpansion
cd /d %~dp0

echo === Oppdaterer vaerdata lokalt ===
py python\build_weather_page.py

echo.
echo === Publiserer til GitHub Pages ===

REM Hent siste endringer fra GitHub (hindrer rejected)
git pull --rebase origin main

REM Legg til alt som skal versjoneres (data/ + index.html + manifest)
git add -A

REM Sjekk om det faktisk er endringer
git diff --cached --quiet
if %errorlevel%==0 (
  echo Ingen endringer aa publisere.
) else (
  REM Lag et timestamp til commit-meldingen
  for /f "tokens=1-3 delims=." %%a in ("%date%") do set D=%%c-%%b-%%a
  for /f "tokens=1-2 delims=:" %%a in ("%time%") do set T=%%a:%%b
  set T=!T: =0!

  git commit -m "Oppdatering !D! !T!"
  git push
  echo Publisert!
)

echo.
pause
