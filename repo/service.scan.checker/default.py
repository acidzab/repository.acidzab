import db_scan
import requests
import xbmc
import xbmcaddon


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def execute_addon_with_builtin(addon_id, params=None):
    builtin_cmd = f'RunAddon({addon_id},{params})' if params else f'RunAddon({addon_id})'
    xbmc.executebuiltin(builtin_cmd, True)


def get_scans(db_params):
    table = db_params.get('table')
    url = f'{db_params.get('scanserver')}/scans/{table}/status'
    scan_status = requests.get(url)
    scan_status.raise_for_status()
    return scan_status.json()


def execute_service():
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)
    monitor = xbmc.Monitor()
    db_params = db_scan.get_db_params()
    while not monitor.waitForAbort(10):
        if monitor.abortRequested():
            break
        current_scans = get_scans(db_params)
        if current_scans and current_scans.get('scan') and not xbmc.getCondVisibility('Library.IsScanningMusic'):
            log('È stata effettuata una scansione, procediamo ad effettuare la scansione')
            execute_addon_with_builtin('service.autoexec.label')


if __name__ == "__main__":
    execute_service()
