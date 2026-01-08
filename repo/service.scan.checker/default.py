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

class WebSocketMonitor:
    def __init__(self):
        self.addon_name = xbmcaddon.Addon().getAddonInfo('name')
        self.monitor = xbmc.Monitor()
        self.db_params = db_scan.get_db_params()

        ws_server = self.db_params.get('scanserver')
        self.ws_url = f"{ws_server}/ws/scans-status"

        self.ws = None
        self.running = True

    def execute_service(self):
        log(self.addon_name)
        log(f"Connessione a: {self.ws_url}")

        # Avvia WebSocket in thread separato
        ws_thread = threading.Thread(target=self.websocket_handler)
        ws_thread.daemon = True
        ws_thread.start()

        # Mantieni il monitor attivo
        while not self.monitor.waitForAbort(1):
            if self.monitor.abortRequested():
                self.running = False
                if self.ws:
                    self.ws.close()
                break

        log("Servizio terminato")

    def websocket_handler(self):
        """Gestisce la connessione WebSocket con reconnect automatico"""
        while self.running:
            try:
                log("Tentativo connessione WebSocket...")
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                    on_open=self.on_open
                )
                self.ws.run_forever()
            except Exception as e:
                log(f"Errore WebSocket: {e}")

            # Reconnect dopo 5 secondi
            if self.running:
                log("Ritento la connessione...")
                xbmc.sleep(5000)

    def on_open(self, ws):
        log("WebSocket connesso!")

    def on_message(self, ws, message):
        """Gestisce i messaggi ricevuti dal server"""
        try:
            log(f"Messaggio ricevuto: {message}")

            # Verifica che non ci sia già una scansione in corso
            if xbmc.getCondVisibility('Library.IsScanningMusic'):
                log("Scansione già in corso, ignoro il comando")
                return

            if message == "scan":
                log('È stata effettuata una scansione, procediamo ad effettuare la scansione')
                execute_addon_with_builtin('service.autoexec.label')

            elif message == "align":
                log('È stato richiesto un allineamento dei dati col db centrale')
                params = db_scan.encode_string(f'?mode=init', safe_chars='()!')
                execute_addon_with_builtin('script.scanner.trigger', params)

        except Exception as e:
            log(f"Errore elaborazione messaggio: {e}")

    def on_error(self, ws, error):
        log(f"Errore WebSocket: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        log(f"WebSocket chiuso")



def execute_service():
    ws_monitor = WebSocketMonitor()
    ws_monitor.execute_service()


if __name__ == "__main__":
    execute_service()
