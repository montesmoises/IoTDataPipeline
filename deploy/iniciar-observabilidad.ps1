# Levanta TODO: Prometheus, Loki, Alloy, Grafana y el recolector.
#
#   .\iniciar-observabilidad.ps1                   arrancar todo
#   .\iniciar-observabilidad.ps1 -Detener          detener todo
#   .\iniciar-observabilidad.ps1 -SinRecolector    solo la observabilidad
#   .\iniciar-observabilidad.ps1 -Obs D:\binarios  binarios en otra ruta
#
# El proyecto se ubica solo (este script vive en <proyecto>\deploy), así que
# funciona sin importar dónde esté clonado el repositorio.
#
# Los binarios van FUERA del repositorio: son cientos de MB y no son código
# nuestro. Por omisión en C:\iot\obs; se cambia con -Obs o con IOT_OBS.

param(
    [switch]$Detener,
    [switch]$SinRecolector,   # levantar solo la observabilidad
    [string]$Obs = $env:IOT_OBS
)

$ErrorActionPreference = "Stop"

$APP    = Split-Path -Parent $PSScriptRoot
$DEPLOY = $PSScriptRoot
if (-not $Obs) { $Obs = "C:\iot\obs" }
$OBS = $Obs.TrimEnd('\')
$GEN = Join-Path $DEPLOY ".generado"

# Ojo con los nombres de proceso: Loki y Alloy corren como
# 'loki-windows-amd64' y 'alloy-windows-amd64', no como 'loki' / 'alloy'.
$procesos = @(
    @{ Nombre = "prometheus";          Exe = Join-Path $OBS "prometheus\prometheus.exe" },
    @{ Nombre = "loki-windows-amd64";  Exe = Join-Path $OBS "loki\loki-windows-amd64.exe" },
    @{ Nombre = "alloy-windows-amd64"; Exe = Join-Path $OBS "alloy\alloy-windows-amd64.exe" },
    @{ Nombre = "grafana";             Exe = Join-Path $OBS "grafana\bin\grafana.exe" }
)

# ── Detener TODO ───────────────────────────────────────────────────────────
if ($Detener) {
    $alguno = $false

    # 1) El recolector (uno o varios procesos de python)
    $rec = Get-CimInstance Win32_Process -Filter "Name like '%python%'" -ErrorAction SilentlyContinue |
           Where-Object { $_.CommandLine -match "Prensas\.py" }
    foreach ($r in $rec) {
        Stop-Process -Id $r.ProcessId -Force -ErrorAction SilentlyContinue
        "  detenido: recolector (PID $($r.ProcessId))"
        $alguno = $true
    }

    # 2) La observabilidad
    foreach ($p in $procesos) {
        $enCurso = Get-Process -Name $p.Nombre -ErrorAction SilentlyContinue
        if ($enCurso) {
            $enCurso | Stop-Process -Force
            "  detenido: $($p.Nombre)"
            $alguno = $true
        }
    }

    # 3) Los plugins de Grafana quedan huérfanos al matar al padre
    Get-Process -ErrorAction SilentlyContinue | Where-Object { $_.Name -like "gpx_*" } | ForEach-Object {
        Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
        "  detenido: $($_.Name) (plugin de Grafana)"
        $alguno = $true
    }

    # 4) Red de seguridad: lo que siga escuchando en nuestros puertos
    Start-Sleep -Seconds 2
    foreach ($puerto in 3000, 9090, 3100, 12345, 9100) {
        Get-NetTCPConnection -LocalPort $puerto -State Listen -ErrorAction SilentlyContinue | ForEach-Object {
            $pr = Get-Process -Id $_.OwningProcess -ErrorAction SilentlyContinue
            if ($pr) {
                Stop-Process -Id $pr.Id -Force -ErrorAction SilentlyContinue
                "  detenido: $($pr.Name) que seguia en el puerto $puerto"
                $alguno = $true
            }
        }
    }

    if (-not $alguno) { "  no habia nada corriendo" }

    Start-Sleep -Seconds 1
    $sigue = @(3000, 9090, 3100, 12345, 9100) | Where-Object {
        (Test-NetConnection localhost -Port $_ -WarningAction SilentlyContinue).TcpTestSucceeded
    }
    if ($sigue) { "  AVISO: siguen abiertos los puertos $($sigue -join ', ')" }
    else { "  todos los puertos cerrados" }
    return
}

"Proyecto : $APP"
"Binarios : $OBS"
""

# ── Verificar los ejecutables ──────────────────────────────────────────────
$faltan = $procesos | Where-Object { -not (Test-Path $_.Exe) }
if ($faltan) {
    Write-Host "Faltan ejecutables:" -ForegroundColor Red
    $faltan | ForEach-Object { Write-Host "  no existe: $($_.Exe)" -ForegroundColor Red }
    Write-Host ""
    Write-Host "Descomprime los ZIP segun la tabla del README, o indica otra ruta:" -ForegroundColor Yellow
    Write-Host "  .\iniciar-observabilidad.ps1 -Obs D:\ruta\a\binarios" -ForegroundColor Yellow
    exit 1
}

# ── Resolver las plantillas ────────────────────────────────────────────────
# Los archivos del repositorio traen __APP__ y __OBS__ en vez de rutas fijas.
# Aquí se escribe la versión con rutas reales en .generado\, que no se versiona:
# así el repositorio queda limpio en todas las máquinas.
New-Item -ItemType Directory -Force $GEN | Out-Null
New-Item -ItemType Directory -Force (Join-Path $GEN "provisioning\dashboards")  | Out-Null
New-Item -ItemType Directory -Force (Join-Path $GEN "provisioning\datasources") | Out-Null

function Resolver($origen, $destino) {
    # __APP_UNIX__ lleva diagonales normales: el archivo de Alloy usa cadenas
    # estilo Go, donde la diagonal invertida es un escape y "C:\Users\..."
    # rompe el parseo. Windows acepta ambas para rutas de archivo.
    $appUnix = $APP.Replace('\', '/')
    (Get-Content $origen -Raw).
        Replace('__APP_UNIX__', $appUnix).
        Replace('__APP__', $APP).
        Replace('__OBS__', $OBS) |
        Set-Content $destino -Encoding UTF8
}

Resolver (Join-Path $DEPLOY "loki.yml")       (Join-Path $GEN "loki.yml")
Resolver (Join-Path $DEPLOY "alloy.alloy")    (Join-Path $GEN "alloy.alloy")
Resolver (Join-Path $DEPLOY "prometheus.yml") (Join-Path $GEN "prometheus.yml")
Copy-Item (Join-Path $DEPLOY "alertas.yml")   (Join-Path $GEN "alertas.yml") -Force
Resolver (Join-Path $DEPLOY "grafana\provisioning\dashboards\dashboards.yml") `
         (Join-Path $GEN "provisioning\dashboards\dashboards.yml")
Copy-Item (Join-Path $DEPLOY "grafana\provisioning\datasources\datasources.yml") `
          (Join-Path $GEN "provisioning\datasources\datasources.yml") -Force
"  configuracion generada en $GEN"

# ── Carpetas de datos (fuera del repositorio) ──────────────────────────────
foreach ($sub in @("datos", "datos\loki", "datos\prometheus", "datos\alloy")) {
    New-Item -ItemType Directory -Force (Join-Path $OBS $sub) | Out-Null
}

# ── Arrancar ───────────────────────────────────────────────────────────────
""
Start-Process (Join-Path $OBS "prometheus\prometheus.exe") -WindowStyle Minimized -ArgumentList @(
    "--config.file=$(Join-Path $GEN 'prometheus.yml')",
    "--storage.tsdb.path=$(Join-Path $OBS 'datos\prometheus')",
    "--storage.tsdb.retention.time=90d"
)
"  Prometheus  -> http://localhost:9090"

Start-Process (Join-Path $OBS "loki\loki-windows-amd64.exe") -WindowStyle Minimized -ArgumentList @(
    "-config.file=$(Join-Path $GEN 'loki.yml')"
)
"  Loki        -> http://localhost:3100"

Start-Sleep -Seconds 3   # Alloy necesita que Loki ya escuche

Start-Process (Join-Path $OBS "alloy\alloy-windows-amd64.exe") -WindowStyle Minimized -ArgumentList @(
    "run", "$(Join-Path $GEN 'alloy.alloy')",
    "--storage.path=$(Join-Path $OBS 'datos\alloy')",
    "--server.http.listen-addr=127.0.0.1:12345"
)
"  Alloy       -> leyendo $APP\logs\*.log   (http://localhost:12345)"

Start-Process (Join-Path $OBS "grafana\bin\grafana.exe") -WindowStyle Minimized -ArgumentList @(
    "server", "--homepath", (Join-Path $OBS "grafana")
)
"  Grafana     -> http://localhost:3000  (admin / admin)"

# ── El recolector ──────────────────────────────────────────────────────────
if (-not $SinRecolector) {
    $yaCorre = Get-CimInstance Win32_Process -Filter "Name like '%python%'" -ErrorAction SilentlyContinue |
               Where-Object { $_.CommandLine -match "Prensas\.py" }
    if ($yaCorre) {
        "  Recolector  -> ya estaba corriendo (PID $($yaCorre.ProcessId -join ', '))"
    } else {
        Start-Process "poetry" -WindowStyle Minimized -WorkingDirectory $APP `
            -ArgumentList "run", "python", "Prensas.py"
        "  Recolector  -> http://localhost:9100/estaciones/lecturas"
    }
}

""
"Grafana necesita saber donde esta la provision (una sola vez por maquina)."
"En $(Join-Path $OBS 'grafana\conf\custom.ini'):"
"  [paths]"
"  provisioning = $(Join-Path $GEN 'provisioning')"
""
"Para detener TODO:  .\iniciar-observabilidad.ps1 -Detener"
