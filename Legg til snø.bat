@echo off
cd /d %~dp0

echo === Registrer snodybde ===
py python\legg_til_sno.py

echo.
echo === Oppdaterer vaerhistorikk og nettside ===
call Oppdater.bat

echo.
echo Ferdig! Du kan no oppdatere sida i nettlesaren.
pause
