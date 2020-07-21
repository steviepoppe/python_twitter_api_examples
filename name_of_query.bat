@echo off

:: Necessary for Unicode support
chcp 65001 >NUL

cd /d C:\python\

:: name of the output CSV file (defaults to the name of this batch file)
set safe_csv=%~n0
set time_zone=Asia/Tokyo
set include_retweets="True"
set add_totals="True"
set double_check="True"

:: name of the input JSON file (defaults to the name of this batch file)
set query=%~n0
set since_id="None"

:: Count total amount of files
for /f %%A in ('dir /A:-D /B .\results\*%query%*.json ^| find /v /c ""') do set total=%%A

:: Necessary to count in loop
setlocal enabledelayedexpansion
set count=0

for %%a in (results/*%query%*.json) do (
:: Create CSV of all relevant query JSON
echo "Pre-processing: %%a"
set /a count += 1
IF !count!==%total% (
py python_parse_tweet_ver2.py %%~na %time_zone% %include_retweets% %safe_csv% %add_totals% %double_check%
) else (
py python_parse_tweet_ver2.py %%~na %time_zone% %include_retweets% %safe_csv%
)

)
endlocal

:: Create metrics CSV files
echo "Calculating metrics..."
py python_metrics.py %safe_csv% %include_retweets%

PAUSE