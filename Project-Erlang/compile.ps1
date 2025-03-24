$erlangFiles = Get-ChildItem -Filter "*.erl"

foreach ($file in $erlangFiles) {
    erlc $file.FullName
    if ($LASTEXITCODE -ne 0) {
        Write-Host ("Compilation failed for " + $file.Name)
        continue
    }
}

Write-Host "Compilation successful!"
exit 0

