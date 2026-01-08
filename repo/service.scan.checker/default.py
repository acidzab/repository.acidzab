import socket
import threading

import db_scan
import websocket
import xbmc
import xbmcaddon



def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def execute_addon_with_builtin(addon_id, params=None):
    builtin_cmd = f'RunAddon({addon_id},{params})' if params else f'RunAddon({addon_id})'
    xbmc.executebuiltin(builtin_cmd, True)

def execute_service():
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)

    monitor = xbmc.Monitor()
    db_params = db_scan.get_db_params()

    # Costruisci URL WebSocket
    ws_url = f"{db_params.get('scanserver')}/ws/scans-status"

    ws = None
    reconnect_counter = 0

    log(f"Servizio avviato, URL: {ws_url}")

    while not monitor.waitForAbort(1):
        if monitor.abortRequested():
            break

        # Gestione connessione
        if ws is None:
            if reconnect_counter <= 0:
                try:
                    log("Connessione WebSocket in corso...")
                    ws = websocket.create_connection(ws_url, timeout=5)
                    ws.settimeout(0.5)  # Timeout recv breve per reattività
                    log("WebSocket connesso!")
                    reconnect_counter = 0
                except Exception as e:
                    log(f"Errore connessione: {e}")
                    ws = None
                    reconnect_counter = 5  # Riprova tra 5 secondi
            else:
                reconnect_counter -= 1
            continue

        # Ricevi messaggi
        try:
            message = ws.recv()

            if message:
                log(f"Messaggio ricevuto: {message}")

                # Verifica che non ci sia già una scansione in corso
                if xbmc.getCondVisibility('Library.IsScanningMusic'):
                    log("Scansione già in corso, ignoro il comando")
                    continue

                if message == "scan":
                    log('È stata effettuata una scansione, procediamo ad effettuare la scansione')
                    if db_params.get('table'):
                        db_scan.reset_scan_status(db_params)
                    execute_addon_with_builtin('service.autoexec.label')

                elif message == "align":
                    log('È stato richiesto un allineamento dei dati col db centrale')
                    if db_params.get('table'):
                        db_scan.reset_scan_status(db_params)
                    params = db_scan.encode_string(f'?mode=init', safe_chars='()!')
                    execute_addon_with_builtin('script.scanner.trigger', params)
                else:
                    log(f"Comando sconosciuto: {message}")

        except socket.timeout:
            # Timeout normale, nessun messaggio disponibile
            pass
        except websocket.WebSocketConnectionClosedException:
            log("WebSocket disconnesso dal server")
            try:
                ws.close()
            except:
                pass
            ws = None
            reconnect_counter = 5
        except Exception as e:
            log(f"Errore ricezione: {e}")
            try:
                ws.close()
            except:
                pass
            ws = None
            reconnect_counter = 5

    # Cleanup finale
    if ws:
        try:
            ws.close()
            log("WebSocket chiuso")
        except Exception as e:
            log(f"Errore chiusura: {e}")

    log("Servizio terminato")

if __name__ == "__main__":
    execute_service()
