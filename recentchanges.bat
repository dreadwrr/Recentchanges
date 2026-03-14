

@echo off

rem rnt

rem If no arguments were passed, add one for parser note for command line use
if "%~1"=="" (
    python "%~dp0src\set_recent_helper.py" run
    exit /b
)

rem search

rem If arguments were provided, prepend or call
python "%~dp0src\set_recent_helper.py" %*
