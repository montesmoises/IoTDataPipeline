# Observabilidad — instalación y arranque

Tres tableros: **Estaciones** (por qué no hay producción), **Partes rechazadas**
y **Logs**. Vienen ya provisionados: al abrir Grafana están ahí.

Todo nativo en Windows. Sin Docker.

---

## Lo primero: ninguno se instala

Los tres son **portables**. Solo se descomprimen y se ejecutan. No pasan por el
instalador de Windows, no tocan el registro, no necesitan permisos de
administrador.

> Grafana ofrece también un `.msi`: **no lo uses**. Descarga el
> *Standalone Windows Binaries* (ZIP).

---

## Dónde va cada cosa

**Los binarios van fuera del repositorio.** Son cientos de MB, no son código
nuestro, y el `.gitignore` los bloquea aunque los descomprimas dentro por error.

```
<donde tengas el proyecto>\          ← el repositorio; puede estar donde sea
├── Prensas.py
├── logs\                            ← Alloy lee de aquí
└── deploy\
    ├── *.yml                        ← plantillas versionadas
    ├── iniciar-observabilidad.ps1
    ├── grafana\                     ← provisión y los 3 tableros
    └── .generado\                   ← configuración con rutas reales (no versionada)

C:\iot\obs\                          ← aquí van los ZIP descomprimidos
├── prometheus\prometheus.exe
├── loki\loki-windows-amd64.exe
├── alloy\alloy-windows-amd64.exe
├── grafana\bin\grafana.exe
└── datos\                           ← lo genera el script
```

### No hace falta mover el proyecto

Grafana, Loki y Alloy exigen rutas absolutas en su configuración, y no
expanden variables. La solución: los `.yml` del repositorio son **plantillas**
con los marcadores `__APP__` y `__OBS__`, y `iniciar-observabilidad.ps1` escribe
la versión resuelta en `deploy\.generado\` cada vez que arranca.

Por eso el repositorio funciona igual esté donde esté, y **queda limpio en todas
las máquinas**: lo generado no se versiona.

Si prefieres los binarios en otra ruta:

```powershell
.\iniciar-observabilidad.ps1 -Obs D:\binarios
# o de forma permanente:
[Environment]::SetEnvironmentVariable("IOT_OBS", "D:\binarios", "User")
```

---

## Paso 1 — el proyecto

```powershell
git clone https://github.com/montesmoises/IoTDataPipeline.git
cd IoTDataPipeline
poetry install
```

Copia tu `.env` a la raíz del proyecto (no viaja por git).

---

## Paso 2 — descargar y descomprimir

Enlaces directos (siempre la última versión, verificados):

| Pieza | Descarga | Peso |
|---|---|---|
| **Loki** | https://github.com/grafana/loki/releases/latest/download/loki-windows-amd64.exe.zip | 45 MB |
| **Alloy** | https://github.com/grafana/alloy/releases/latest/download/alloy-windows-amd64.exe.zip | 106 MB |
| Prometheus | https://prometheus.io/download/ → `windows-amd64` | ~100 MB |
| Grafana | https://grafana.com/grafana/download?platform=windows → **Standalone Windows Binaries** | ~120 MB |

> ### Alloy sustituye a Promtail
> Grafana **descontinuó Promtail**: ya no se publica en los releases actuales de
> Loki (el enlace a `latest` devuelve 404). Alloy hace lo mismo —leer archivos de
> log y enviarlos a Loki— y es la pieza que sí tiene soporte. Por eso la
> configuración es `alloy.alloy` y no `promtail.yml`.
>
> Si por alguna razón necesitas Promtail, sigue disponible fijando una versión
> antigua: `https://github.com/grafana/loki/releases/download/v3.4.2/promtail-windows-amd64.exe.zip`

Dónde descomprimir cada uno:

| ZIP | Descomprimir en | Debe quedar |
|---|---|---|
| Prometheus | `C:\iot\obs\prometheus\` | `prometheus.exe` |
| Loki | `C:\iot\obs\loki\` | `loki-windows-amd64.exe` |
| Alloy | `C:\iot\obs\alloy\` | `alloy-windows-amd64.exe` |
| Grafana | `C:\iot\obs\grafana\` | `bin\grafana.exe` |

Los ZIP traen una carpeta dentro (`prometheus-2.5x.windows-amd64\`). **Saca el
contenido**: el `.exe` debe quedar directamente en `C:\iot\obs\prometheus\`, sin
carpeta anidada.

---

## Paso 3 — preparar Grafana (una sola vez por máquina)

**a) Instalar el plugin Infinity**, que es el que lee el JSON del servicio:

```powershell
C:\iot\obs\grafana\bin\grafana.exe cli --homepath C:\iot\obs\grafana `
    --pluginsDir C:\iot\obs\grafana\data\plugins `
    plugins install yesoreyeram-infinity-datasource
```

> Desde Grafana 10 ya **no existe `grafana-cli.exe`**: el CLI viene dentro del
> mismo binario, por eso es `grafana.exe cli`.

**b) Crear `C:\iot\obs\grafana\conf\custom.ini`.** La ruta de provisión depende
de dónde tengas el proyecto, y el script te la imprime al arrancar:

```ini
[paths]
provisioning = <ruta-del-proyecto>\deploy\.generado\provisioning

[users]
default_theme = dark

[security]
admin_password = admin
```

Corre una vez `.\iniciar-observabilidad.ps1` y copia la ruta exacta que aparece
al final. Después reinicia Grafana.

---

## Paso 4 — arrancar

```powershell
cd <ruta-del-proyecto>\deploy
.\iniciar-observabilidad.ps1
```

El script hace cuatro cosas: verifica los ejecutables, resuelve las plantillas
con las rutas reales, crea las carpetas de datos y levanta los cuatro procesos
minimizados.

Para bajarlos:

```powershell
.\iniciar-observabilidad.ps1 -Detener
```

**El recolector va aparte**, porque es el que habla con los PLCs:

```powershell
cd <ruta-del-proyecto>
poetry run python Prensas.py
```

---

## Entrar

**http://localhost:3000** — `admin` / `admin`.

Los tres tableros están en la carpeta **IoT Planta** y están enlazados:

- En **Estaciones**, clic en el nombre → abre **Logs** filtrado a esa estación
- Clic en la columna **IP** → abre `/debug/plc/<ip>` con los words crudos
- En **Rechazos**, clic en la estación → también salta a sus logs

---

## Al pasar al servidor

**No hay que editar ninguna configuración**, esté donde esté el proyecto:

1. `git clone` donde quieras y `poetry install`
2. Copiar el `.env` de producción
3. Descomprimir los mismos cuatro ZIP en `C:\iot\obs\`
4. Repetir el paso 3 (plugin y `custom.ini`) — eso sí es por máquina
5. `.\iniciar-observabilidad.ps1`

El script resuelve las rutas solo. Lo único que cambia entre máquinas es la
línea `provisioning` del `custom.ini`, que el propio script te imprime.

### Para que arranquen solos

En el servidor conviene registrarlos como servicios de Windows con
[NSSM](https://nssm.cc/) (también portable, sin instalador):

Usa las rutas que imprime `iniciar-observabilidad.ps1` (las de `.generado`):

```powershell
$nssm = "C:\iot\obs\nssm\nssm.exe"
$gen  = "<ruta-del-proyecto>\deploy\.generado"

& $nssm install IoT-Prometheus C:\iot\obs\prometheus\prometheus.exe `
    "--config.file=$gen\prometheus.yml --storage.tsdb.path=C:\iot\obs\datos\prometheus"

& $nssm install IoT-Loki  C:\iot\obs\loki\loki-windows-amd64.exe  "-config.file=$gen\loki.yml"

& $nssm install IoT-Alloy C:\iot\obs\alloy\alloy-windows-amd64.exe `
    "run $gen\alloy.alloy --storage.path=C:\iot\obs\datos\alloy"

& $nssm install IoT-Recolector `
    "C:\Users\<usuario>\AppData\Local\pypoetry\Cache\virtualenvs\<venv>\Scripts\python.exe" `
    "<ruta-del-proyecto>\Prensas.py"
& $nssm set IoT-Recolector AppDirectory "<ruta-del-proyecto>"
```

Como los servicios no ejecutan el script de arranque, hay que correrlo **una
vez** antes de registrarlos para que `.generado` exista.

El recolector como servicio es lo que resuelve el problema de fondo: hoy, si
alguien cierra la sesión de Windows, deja de contarse producción.

---

## Qué mira cada tablero

| Tablero | Fuente | Para qué |
|---|---|---|
| Estaciones | Infinity (`/estaciones/estado`) + Prometheus | Por qué una estación no registra |
| Partes rechazadas | Infinity (`/partes-rechazadas`) + Prometheus | Qué número falló, cuántas veces y por qué |
| Logs | Loki | El detalle, con todas las estaciones a la vez |

Prometheus **no guarda logs** — eso es Loki. Y el detalle textual (número de
parte, bloque crudo) no va en métricas por cardinalidad: vive en el JSON del
servicio y en los logs.

---

## Alertas

`alertas.yml` trae seis reglas, entre ellas la que sustituye la revisión manual:

```yaml
- alert: SinProduccionConPLCSano
  expr: rate(produccion_piezas_total[15m]) == 0
        and on(estacion) (estacion_motivo{motivo="PRODUCIENDO"} == 0)
  for: 20m
```

Se ven en http://localhost:9090/alerts. Para que **avisen** por correo o Teams
falta configurar un canal en Grafana Alerting.

---

## Diagnóstico rápido, sin Grafana

```powershell
curl "http://localhost:9100/estaciones/estado?solo_problemas=1"
curl http://localhost:9100/debug/plc/10.1.2.1
curl http://localhost:9100/debug/plc
```
