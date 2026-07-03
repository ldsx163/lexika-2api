@echo off
cd /d "%~dp0"
pip install -r requirements.txt 2>nul
python server.py
pause
