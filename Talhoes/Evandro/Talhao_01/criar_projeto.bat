@echo off

pip install rasterio

REM Caminho do diretório onde o script Python está localizado
SET DIR=%~dp0

REM Caminho completo para o script Python
SET SCRIPT_PATH=%DIR%Criar_Projeto.py

REM Executar o QGIS com o script Python
"C:\Program Files\QGIS 3.34.4\bin\qgis-ltr-bin.exe" --code "%SCRIPT_PATH%"

pause
