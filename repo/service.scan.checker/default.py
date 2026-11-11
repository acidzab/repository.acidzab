import json

import db_scan
import requests
import xbmc
import xbmcaddon
from requests import auth

headers = {
    "Accept": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
}
db_params = db_scan.get_db_params()
scan_server_url = f"{db_params.get('scanserver')}/scans/status"
sse_auth = auth.HTTPBasicAuth(db_params.get('scanuser'), db_params.get('scanpass'))

# Backoff tra i tentativi di riconnessione (secondi)
initial_delay = 5
max_delay = 60


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def execute_addon_with_builtin(addon_id, params=None):
    builtin_cmd = f'RunAddon({addon_id},{params})' if params else f'RunAddon({addon_id})'
    xbmc.executebuiltin(builtin_cmd, True)


def open_sse_channel():
    # read timeout > intervallo di keepalive del server (5s): se non arriva
    # nulla per 30s la connessione è considerata morta (es. half-open)
    sse_channel = requests.get(scan_server_url, headers=headers, stream=True, timeout=(5, 30), auth=sse_auth)
    sse_channel.raise_for_status()

    log(f"Servizio avviato, URL: {scan_server_url}")
    return sse_channel


def close_sse_channel(sse_channel):
    try:
        sse_channel.close()
    except Exception:
        pass


def handle_message(message):
    # Verifica che non ci sia già una scansione in corso
    if xbmc.getCondVisibility('Library.IsScanningMusic'):
        log("Scansione già in corso, ignoro il comando")
        return
    operation = message.get('operation')
    paths = message.get('paths')
    if operation == "scan":
        log('È stata effettuata una scansione, procediamo ad effettuare la scansione')
        execute_addon_with_builtin('service.autoexec.library.sync')
    elif operation == "align":
        log('È stato richiesto un allineamento dei dati col db centrale')
        params = db_scan.encode_string('?mode=init', safe_chars='()!')
        if paths:
            query_string = ';'.join([f"path={path}" for path in paths if path])
            params = db_scan.encode_string(f'?{query_string};mode={operation}', safe_chars='()!')
        execute_addon_with_builtin('script.scanner.trigger', params)
    else:
        log(f"Comando sconosciuto: {message}")


def execute_service():
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)
    monitor = xbmc.Monitor()
    delay = initial_delay

    while not monitor.abortRequested():
        sse_channel = None
        try:
            sse_channel = open_sse_channel()
            delay = initial_delay  # connessione riuscita: reset del backoff

            for line in sse_channel.iter_lines(decode_unicode=True):
                if monitor.abortRequested():
                    break
                if not line:
                    continue
                line = line.strip()

                # ignora heartbeat
                if line.startswith(":"):
                    continue

                if line.startswith('data:'):
                    try:
                        message = json.loads(line[5:].strip())
                    except ValueError as e:
                        log(f"Messaggio SSE non valido: {e}")
                        continue
                    handle_message(message)

            log("Stream chiuso dal server")
        except Exception as e:
            log(f"Errore ricezione SSE: {e}")
        finally:
            if sse_channel is not None:
                close_sse_channel(sse_channel)

        # attesa prima della riconnessione, interrompibile alla chiusura di Kodi
        log(f"Riconnessione con lo scan poller tra {delay}s")
        if monitor.waitForAbort(delay):
            break
        delay = min(delay * 2, max_delay)

    log("Servizio terminato")


if __name__ == "__main__":
    execute_service()