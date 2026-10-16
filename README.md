# 🚀 DataSync Manager: SFTP ➔ S3 / FTP Manager con Rclone y SQLite

Aplicación web desarrollada en Python con FastAPI y Rclone para la sincronización continua, mapeo y transferencia resiliente de datos desde servidores SFTP hacia buckets de almacenamiento S3 (AWS S3, MinIO, Cloudflare R2, Wasabi) o servidores FTP estándar.

---

## 🌟 Características Principales

1. **Entorno Gráfico Web Moderno e Intuitivo**:
   - Tablero de control con métricas en tiempo real (Archivos Totales, Enviados con Éxito, Enviándose, Con Error, Pendientes).
   - Barra de control de sincronización con estados: **EJECUTANDO**, **PAUSADO** y **DETENIDO**.
   - Tabla interactiva con búsqueda en tiempo real, filtros por estado y paginación.
   - Visor de errores con modal detallado que muestra la causa exacta reportada por el comando Rclone.
   - Botón de reintento individual y reintento masivo (*"Reintentar todos los errores"*).

2. **Integración con Rclone**:
   - Generación dinámica y segura de `data/rclone.conf` con ofuscación de contraseñas (`rclone obscure`).
   - Mapeo de archivos remotos vía `rclone lsjson --files-only` (soporte para escaneo recursivo de subdirectorios).
   - Transferencia granular archivo por archivo mediante `rclone copyto` para aislamiento de errores e informe de progreso.
   - Selector configurable entre **Bucket S3** (AWS, Cloudflare R2, MinIO, Wasabi, Ceph) o **Servidor FTP**.
   - Botones para **probar conexión** tanto en el origen SFTP como en el destino S3/FTP antes de guardar.

3. **Mapeo y Auditoría en Base de Datos SQLite**:
   - Base de datos local transaccional con modo WAL (`PRAGMA journal_mode=WAL;`).
   - Estados de archivo rastreados:
     - `pending`: Mapeado desde SFTP, en cola de transferencia.
     - `transferring`: Enviándose activamente en segundo plano.
     - `completed`: Enviado exitosamente al destino.
     - `error`: Fallo de transferencia con mensaje de error guardado en SQLite.
   - Detección de cambios: si un archivo previamente enviado es modificado en el SFTP (cambio en tamaño o fecha de modificación), se reencola automáticamente para actualización.

4. **Automatización y Tareas Periódicas**:
   - Planificador en segundo plano (`asyncio`) que ejecuta ciclos de escaneo y envío a intervalos configurables (por ejemplo, cada 60 segundos).
   - Botón *"Sincronizar Ahora"* para forzar una sincronización manual inmediata.

5. **Contingencia y Resiliencia ante Reinicios**:
   - **Recuperación tras caídas**: Si el servidor se apaga o reinicia mientras un archivo está enviándose (`transferring`), la rutina de inicio sanea automáticamente la base de datos devolviendo los archivos interrumpidos a `pending` y auditando el evento en los logs.
   - **Autoinicio en el Sistema**: Si la sincronización estaba activa o tiene configurado `auto_start`, el servicio reanuda la sincronización sin necesidad de abrir la interfaz.
   - **Instalador de Servicio del Sistema Operativo**: Compatible con **LaunchAgent** en macOS y **Systemd** en Linux para arrancar automáticamente al encender el equipo.

6. **Controles de Tarea**:
   - **Iniciar**: Activa el planificador periódico y comienza la sincronización.
   - **Pausar**: Suspende la toma de nuevos archivos hasta que sea reanudado.
   - **Reanudar**: Continúa el procesamiento de la cola pendiente.
   - **Detener**: Detiene el worker inmediatamente y cancela de forma limpia cualquier subproceso Rclone activo.

---

## 📁 Estructura del Proyecto

```
data_sync_manager/
│
├── app/
│   ├── __init__.py
│   ├── config.py             # Rutas, directorios y configuración base
│   ├── database.py           # Conexión SQLite, esquema WAL y contingencia
│   ├── rclone_manager.py     # Generación de rclone.conf, pruebas de red y transferencias
│   ├── sync_engine.py        # Orquestador del ciclo de sincronización y controles
│   ├── api/
│   │   ├── __init__.py
│   │   └── routes.py         # Endpoints REST para API y UI
│   ├── static/               # Archivos estáticos
│   └── templates/
│       └── index.html        # Interfaz web gráfica moderna (Tailwind CSS + Lucide)
│
├── data/                     # Almacenamiento persistente
│   ├── sync_data.db          # Base de datos SQLite
│   └── rclone.conf           # Configuración activa de remotos
│
├── scripts/
│   ├── start.sh              # Script de arranque rápido
│   ├── install_service.sh    # Instalador de servicio en sistema operativo
│   └── demo_simulation.py    # Script de prueba y simulación end-to-end
│
├── main.py                   # Punto de entrada de la aplicación FastAPI / Uvicorn
├── requirements.txt          # Dependencias de Python
└── test_app.py               # Suite de pruebas unitarias y de integración
```

---

---

## ⚙️ Requisitos Previos

- **Python 3.10 o superior** (asegúrate de marcar *"Add Python to PATH"* si estás en Windows).
- *(Opcional)* **Rclone**: ¡No te preocupes si no lo tienes instalado! El instalador automático detecta tu sistema operativo (Mac, Linux o Windows) y descarga automáticamente el binario oficial correspondiente en la carpeta local `bin/`.

---

## 🚀 Instalación Automatizada (Mac, Linux y Windows)

El proyecto incluye scripts de automatización completa que descargan e instalan dependencias de Python, configuran Rclone según tu sistema operativo e inicializan la base de datos SQLite:

### 🪟 En Windows:
1. Haz doble clic en **`install.bat`** (o ejecuta en PowerShell: `.\install.ps1`).
2. Para iniciar la aplicación, haz doble clic en **`start.bat`** (o ejecuta `python main.py`).

### 🍏 En macOS:
1. Abre tu terminal y ejecuta:
   ```bash
   chmod +x install.sh scripts/start.sh
   ./install.sh
   ```
2. Para iniciar la aplicación:
   ```bash
   ./scripts/start.sh
   # o bien: python3 main.py
   ```

### 🐧 En Linux (Ubuntu, Debian, RedHat, etc.):
1. En tu terminal ejecuta:
   ```bash
   chmod +x install.sh scripts/start.sh
   ./install.sh
   ```
2. Para iniciar la aplicación:
   ```bash
   ./scripts/start.sh
   # o bien: python3 main.py
   ```

### 🌐 Acceso a la Interfaz Web:
Una vez iniciada la app, abre tu navegador en:
👉 **`http://localhost:8000`**

---

## 🔧 Configuración de Remotos

En la pestaña **"Configuración de Remotos"** de la interfaz web:

### 1. Origen SFTP
- **Host o IP**: Servidor SFTP (ej. `sftp.tuempresa.com`)
- **Puerto**: `22` (o puerto personalizado)
- **Usuario**: Usuario de autenticación
- **Contraseña o Llave Privada SSH**: Puedes ingresar contraseña (se protegerá con `rclone obscure`) o la ruta absoluta a la clave SSH (`/home/usuario/.ssh/id_rsa`).
- **Directorio Remoto**: Carpeta específica en el SFTP (dejar vacío para la raíz).
- Haz clic en **"Probar Conexión"** para validar antes de guardar.

### 2. Destino: Bucket S3 o Servidor FTP
- **Si eliges S3**:
  - Proveedor: `AWS`, `Cloudflare R2`, `MinIO`, `Wasabi`, `Ceph` u `Otro`.
  - Nombre del Bucket: Nombre exacto de tu bucket (ej. `mi-bucket-datos`).
  - Access Key ID y Secret Access Key.
  - Región (ej. `us-east-1`).
  - Endpoint personalizado: Requerido para MinIO o Cloudflare R2 (ej. `https://<account_id>.r2.cloudflarestorage.com`).
  - Carpeta/Prefijo: Subdirectorio destino en el bucket.
- **Si eliges FTP**:
  - Host FTP, Puerto (21), Usuario, Contraseña y Carpeta destino.
- Haz clic en **"Probar Destino"** para validar permisos y conectividad.

### 3. Programación Periódica
- **Intervalo de sincronización**: Intervalo en segundos (por defecto `60`).
- **Auto-iniciar al encender el equipo**: Marcado por defecto para garantizar contingencia continua.
- **Escaneo recursivo**: Activar para escanear carpetas y subcarpetas anidadas en el SFTP.
- Haz clic en **"Guardar Cambios de Configuración"**.

---

## 🛡️ Contingencia ante Reinicio de la Máquina

El sistema incluye una triple capa de protección:

1. **Durabilidad en SQLite (WAL)**: Registros atómicos y seguros contra pérdidas de energía.
2. **Auto-Recuperación al Arrancar**:
   - Si la máquina se apaga inesperadamente mientras se estaba enviando un archivo, dicho archivo queda registrado como `transferring`.
   - Al encender y reiniciar la app, el motor detecta automáticamente la interrupción, restaura el estado a `pending` y audita el evento con el mensaje:
     > *"Interrumpido por reinicio o cierre del sistema. Reencolado automáticamente."*
3. **Servicio del Sistema Operativo**:
   - En la pestaña **"Contingencia & Reinicios"** de la web, haz clic en **"Instalar como Servicio de Inicio del Sistema"** o ejecuta:
     ```bash
     ./scripts/install_service.sh
     ```
   - Esto creará un servicio en `launchd` (macOS) o `systemd` (Linux) para que la aplicación inicie sola en segundo plano al arrancar la computadora, sin necesidad de que abras una terminal.

---

## 🧪 Pruebas Automatizadas

Para ejecutar la suite de pruebas unitarias y de integración:

```bash
python3 -m unittest -v test_app.py
```

Las pruebas cubren:
- Inicialización y consistencia de base de datos SQLite.
- Contingencia ante reinicio y recuperación de transferencias interrumpidas.
- Mapeo de archivos y detección de modificaciones en origen.
- Transiciones de estado (`pending` ➔ `transferring` ➔ `completed` / `error`) y reintentos.
- Generación dinámica de `rclone.conf` y ofuscación de credenciales.
- Controles del motor (Iniciar, Pausar, Reanudar, Detener).
- Validación de todos los endpoints de la API REST.

