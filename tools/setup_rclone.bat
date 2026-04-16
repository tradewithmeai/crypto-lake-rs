@echo off
setlocal EnableDelayedExpansion

:: ============================================================
:: setup_rclone.bat — Download rclone and configure Google Drive
:: ============================================================
echo.
echo  Crypto Lake — rclone Setup
echo  ==========================
echo.

set TOOLS_DIR=%~dp0
set RCLONE_EXE=%TOOLS_DIR%rclone.exe
set RCLONE_ZIP=%TOOLS_DIR%rclone-tmp.zip
set RCLONE_URL=https://downloads.rclone.org/rclone-current-windows-amd64.zip

:: Check if rclone already installed
if exist "%RCLONE_EXE%" (
    echo  [OK] rclone.exe already exists in tools\
    goto :configure
)

where rclone >nul 2>&1
if %errorlevel% == 0 (
    echo  [OK] rclone found in PATH
    goto :configure
)

:: Download rclone
echo  Downloading rclone for Windows (64-bit)...
echo  From: %RCLONE_URL%
echo.

powershell -Command "& { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '%RCLONE_URL%' -OutFile '%RCLONE_ZIP%' }"
if %errorlevel% neq 0 (
    echo.
    echo  ERROR: Download failed. Check your internet connection and try again.
    pause
    exit /b 1
)

:: Extract rclone.exe from the zip
echo  Extracting rclone.exe...
powershell -Command "& { $zip = [IO.Compression.ZipFile]::OpenRead('%RCLONE_ZIP%'); $entry = $zip.Entries | Where-Object { $_.Name -eq 'rclone.exe' } | Select-Object -First 1; if ($entry) { [IO.Compression.ZipFileExtensions]::ExtractToFile($entry, '%RCLONE_EXE%', $true); Write-Host 'Extracted OK' } else { Write-Host 'ERROR: rclone.exe not found in zip'; exit 1 }; $zip.Dispose() }"
if %errorlevel% neq 0 (
    echo  ERROR: Extraction failed.
    del /q "%RCLONE_ZIP%" 2>nul
    pause
    exit /b 1
)

del /q "%RCLONE_ZIP%" 2>nul
echo  [OK] rclone.exe saved to tools\
echo.

:configure
echo  ============================================================
echo  Now we will configure Google Drive access.
echo.
echo  When prompted:
echo    1. Type "n" to create a new remote
echo    2. Name it "gdrive" (exactly, lowercase)
echo    3. Choose "drive" from the storage type list
echo    4. Leave client_id and client_secret blank (press Enter)
echo    5. Choose scope 1 (full access)
echo    6. Leave root_folder_id blank
echo    7. Leave service_account_file blank
echo    8. Choose "n" for advanced config
echo    9. Choose "y" to use auto config (browser will open)
echo   10. Sign in with your Google Workspace account
echo   11. Choose "n" (not a Shared Drive)
echo   12. Confirm with "y"
echo.
echo  Press any key to start rclone config...
pause >nul

if exist "%RCLONE_EXE%" (
    "%RCLONE_EXE%" config
) else (
    rclone config
)

echo.
echo  ============================================================
echo  Setup complete!
echo.
echo  Next steps:
echo    python tools\archive.py setup    -- verify everything works
echo    python tools\archive.py status   -- see local vs Drive summary
echo    python tools\archive.py sync     -- upload data to Google Drive
echo  ============================================================
echo.
pause
