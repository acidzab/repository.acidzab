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
    try:
        scan_status = requests.get(url)
        scan_status.raise_for_status()
    except requests.exceptions.RequestException:
        return None
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
        scan_detected = current_scans and current_scans.get('scan') and not xbmc.getCondVisibility(
            'Library.IsScanningMusic')
        align_detected = current_scans and current_scans.get('align') and not xbmc.getCondVisibility(
            'Library.IsScanningMusic')
        if scan_detected:
            log('È stata effettuata una scansione, procediamo ad effettuare la scansione')
            if db_params.get('table'):
                db_scan.reset_scan_status(db_params)
            execute_addon_with_builtin('service.autoexec.label')
        if align_detected:
            log('È stato richiesto un allineamento dei dati col db centrale')
            if db_params.get('table'):
                db_scan.reset_scan_status(db_params)
            params = db_scan.encode_string(f'?mode=init', safe_chars='()!')
            execute_addon_with_builtin('script.scanner.trigger', params)


if __name__ == "__main__":
    execute_service()
