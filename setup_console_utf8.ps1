# Setup PowerShell console for UTF-8 encoding
# Run this script to configure UTF-8 encoding for the current session
# Or add this to your PowerShell profile for permanent configuration

# Set console output encoding to UTF-8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::InputEncoding = [System.Text.Encoding]::UTF8

# Set PowerShell output encoding
$OutputEncoding = [System.Text.Encoding]::UTF8

# Change code page to UTF-8 (65001)
chcp 65001 | Out-Null

# Set default encoding for .NET operations
[System.Text.Encoding]::Default = [System.Text.Encoding]::UTF8

Write-Host "Console encoding configured for UTF-8" -ForegroundColor Green
Write-Host "OutputEncoding: $([Console]::OutputEncoding.EncodingName)" -ForegroundColor Cyan
Write-Host "InputEncoding: $([Console]::InputEncoding.EncodingName)" -ForegroundColor Cyan
Write-Host "Code page: $(chcp.com | Select-String -Pattern '\d+')" -ForegroundColor Cyan

# Test UTF-8 encoding
$test = "лапки"
Write-Host "`nTest string: $test" -ForegroundColor Yellow
$testBytes = [System.Text.Encoding]::UTF8.GetBytes($test)
Write-Host "UTF-8 bytes: $($testBytes -join ' ')" -ForegroundColor Yellow
Write-Host "Expected: 208 187 208 176 208 191 208 186 208 184" -ForegroundColor Gray

# Verify encoding is correct
$expectedBytes = @(208, 187, 208, 176, 208, 191, 208, 186, 208, 184)
if (Compare-Object $testBytes $expectedBytes) {
    Write-Host "WARNING: Encoding mismatch detected!" -ForegroundColor Red
} else {
    Write-Host "Encoding test passed!" -ForegroundColor Green
}
