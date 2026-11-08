# ==============================================================================
# Script tự động dừng toàn bộ cụm Microservices (Ports 8080 - 8085)
# ==============================================================================

Write-Host "==============================================================================" -ForegroundColor Yellow
Write-Host "🛑 ĐANG DỪNG CỤM MICROSERVICES (Ports 8080, 8081, 8082, 8083, 8084, 8085)..." -ForegroundColor Yellow
Write-Host "==============================================================================" -ForegroundColor Yellow

$ports = @(8080, 8081, 8082, 8083, 8084, 8085)
$killedCount = 0

foreach ($port in $ports) {
    try {
        $connections = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
        if ($connections) {
            foreach ($conn in $connections) {
                $processId = $conn.OwningProcess
                if ($processId -and $processId -gt 0) {
                    $proc = Get-Process -Id $processId -ErrorAction SilentlyContinue
                    if ($proc) {
                        Write-Host "  -> Đang dừng tiến trình '$($proc.ProcessName)' (PID: $processId) đang lắng nghe cổng $port..." -ForegroundColor Red
                        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
                        $killedCount++
                    }
                }
            }
        }
    } catch {
        # ignore
    }
}

Write-Host "`n✅ Đã giải phóng $killedCount tiến trình microservice thành công!" -ForegroundColor Green
Write-Host "💡 Lưu ý: Cụm Docker Kafka (localhost:9092) vẫn đang giữ nguyên. Nếu muốn tắt Kafka, chạy: docker compose down" -ForegroundColor Cyan

