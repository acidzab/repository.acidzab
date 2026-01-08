import time

import db_scan
import requests
import xbmc
import xbmcaddon

headers = {
    "Accept": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
}
db_params = db_scan.get_db_params()
scan_server_url = f"{db_params.get('scanserver')}/scans/status"


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def execute_addon_with_builtin(addon_id, params=None):
    builtin_cmd = f'RunAddon({addon_id},{params})' if params else f'RunAddon({addon_id})'
    xbmc.executebuiltin(builtin_cmd, True)


def open_sse_channel():
    sse_channel = requests.get(scan_server_url, headers=headers, stream=True, timeout=(5, None))
    sse_channel.raise_for_status()

    log(f"Servizio avviato, URL: {scan_server_url}")
    return sse_channel


def retry_sse_channel(sse_channel):
    log("Riconnessione con lo scan poller")
    close_sse_channel(sse_channel)
    time.sleep(5)
    try:
        sse_channel = open_sse_channel()
    except Exception as e:
        log(f'Errore durante la riconnessione {e}')
    return sse_channel


def close_sse_channel(sse_channel):
    try:
        sse_channel.close()
    except Exception:
        pass


def execute_service():
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)
    sse_channel = open_sse_channel()
    monitor = xbmc.Monitor()

    while not monitor.waitForAbort():
        try:
            for message in sse_channel.iter_lines(decode_unicode=True):
                if monitor.abortRequested():
                    break
                if not message:
                    continue
                message = message.strip()
                if message:
                    log(f"Messaggio ricevuto: {message}")

                    # Verifica che non ci sia già una scansione in corso
                    if xbmc.getCondVisibility('Library.IsScanningMusic'):
                        log("Scansione già in corso, ignoro il comando")
                        continue

                    if message == "scan":
                        log('È stata effettuata una scansione, procediamo ad effettuare la scansione')
                        execute_addon_with_builtin('service.autoexec.label')

                    elif message == "align":
                        log('È stato richiesto un allineamento dei dati col db centrale')
                        params = db_scan.encode_string(f'?mode=init', safe_chars='()!')
                        execute_addon_with_builtin('script.scanner.trigger', params)
                    else:
                        log(f"Comando sconosciuto: {message}")
        except Exception as e:
            log(f"Errore ricezione SSE: {e}")
            retry_sse_channel(sse_channel)

    log("Servizio terminato")


if __name__ == "__main__":
    execute_service()
