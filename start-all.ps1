# ==============================================================================
# Script tự động khởi động toàn bộ cụm Microservices Event-Driven trên Windows
# ==============================================================================

Write-Host "==============================================================================" -ForegroundColor Cyan
Write-Host "🚀 KHỞI ĐỘNG CỤM MICROSERVICES E-COMMERCE EVENT-DRIVEN (SPRING BOOT + KAFKA)" -ForegroundColor Cyan
Write-Host "==============================================================================" -ForegroundColor Cyan

# 1. Kiểm tra Kafka Docker
Write-Host "`n[1/3] Kiểm tra Kafka Broker (Port 9092)..." -ForegroundColor Yellow
$kafkaPortTest = Test-NetConnection -ComputerName localhost -Port 9092 -InformationLevel Quiet -WarningAction SilentlyContinue

if (-not $kafkaPortTest) {
    Write-Host "⚠️ Kafka Broker chưa chạy. Đang tự động chạy 'docker compose up -d'..." -ForegroundColor Yellow
    docker compose up -d
    Start-Sleep -Seconds 5
} else {
    Write-Host "✅ Kafka Broker đang chạy tại localhost:9092!" -ForegroundColor Green
}

# 2. Danh sách 6 Microservices cần chạy
$services = @(
    @{ Name = "order-service"; Port = 8081; Title = "1. ORDER SERVICE (:8081)" },
    @{ Name = "inventory-service"; Port = 8082; Title = "2. INVENTORY SERVICE (:8082)" },
    @{ Name = "payment-service"; Port = 8083; Title = "3. PAYMENT SERVICE (:8083)" },
    @{ Name = "fraud-detection-service"; Port = 8084; Title = "4. FRAUD STREAMS (:8084)" },
    @{ Name = "notification-service"; Port = 8085; Title = "5. NOTIFICATION SERVICE (:8085)" },
    @{ Name = "api-gateway"; Port = 8080; Title = "6. API GATEWAY & DASHBOARD (:8080)" }
)

Write-Host "`n[2/3] Đang khởi động 6 Microservices..." -ForegroundColor Yellow

$scriptRoot = $PSScriptRoot

foreach ($s in $services) {
    Write-Host "  -> Khởi động $($s.Title)..." -ForegroundColor Cyan
    $cmd = "cd '$scriptRoot'; `$host.UI.RawUI.WindowTitle = '$($s.Title)'; .\mvnw.cmd spring-boot:run -pl $($s.Name)"
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd
    Start-Sleep -Seconds 2
}

# 3. Hoàn tất & Hướng dẫn truy cập
Write-Host "`n[3/3] Hoàn tất quá trình khởi động!" -ForegroundColor Green
Write-Host "==============================================================================" -ForegroundColor Cyan
Write-Host "🌐 MISSION CONTROL WEB DASHBOARD: http://localhost:8080" -ForegroundColor Green
Write-Host "📖 SWAGGER DOCUMENTATION:        http://localhost:8080/swagger-ui.html" -ForegroundColor Green
Write-Host "📊 KAFKA UI DASHBOARD:            http://localhost:8085" -ForegroundColor Green
Write-Host "==============================================================================" -ForegroundColor Cyan
Write-Host "💡 Để dừng toàn bộ các service, hãy chạy: .\stop-all.ps1" -ForegroundColor Gray

