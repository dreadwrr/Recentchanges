@echo off

rem rnt invert search switch
set PYTHONUNBUFFERED="1"
set "CMD_LINE=%~f0"
rem If no arguments were passed, just prepend and call
rem 300 seconds are used as no args means 5 minutes
if "%~1"=="" (
    python "%~dp0src\set_recent_helper.py" 300 inv
    exit /b
)

rem search
rem If arguments were provided, prepend and pass arguments and call
python "%~dp0src\set_recent_helper.py" %* inv