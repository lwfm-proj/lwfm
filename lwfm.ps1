# PowerShell equivalent of lwfm.sh

# Set environment variable for Flask app
$env:FLASK_APP = "lwfm/midware/impl/LwfmEventSvc"

# Start Flask server on port 3000 in the background
$flaskProc = Start-Process -FilePath "flask" -ArgumentList "run", "-p", "3000" -PassThru

# Start tailing the repo file in a background process
$repoFile = Join-Path $HOME ".lwfm/lwfm.repo"
if (Test-Path $repoFile) {
    $tailProc = Start-Process -FilePath "powershell" -ArgumentList "-NoProfile", "-Command", "Get-Content '$repoFile' -Tail 20 -Wait" -PassThru
}

try {
    # Keep-alive: Wait until user interrupts (Ctrl+C) or window is closed
    while ($true) {
        Start-Sleep -Seconds 1
    }
}
finally {
    # Kill the Flask process if it is still running
    if ($flaskProc -and !$flaskProc.HasExited) {
        Write-Host "Stopping Flask server..."
        $flaskProc | Stop-Process -Force
    }
    # Kill the tail process if it is still running
    if ($tailProc -and !$tailProc.HasExited) {
        Write-Host "Stopping log tail..."
        $tailProc | Stop-Process -Force
    }
}
