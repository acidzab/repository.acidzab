import sqlite3

import db_scan
import xbmc
import xbmcaddon


class ScanMonitor(xbmc.Monitor):
    def __init__(self):
        super(ScanMonitor, self).__init__()
        self.scan_finished = False

    def onNotification(self, sender, method, data):
        if sender == 'script.scanner.trigger' and method == 'Other.OnScanAndAlignFinished':
            self.scan_finished = True

    def wait_for_scan(self):
        """Attende il completamento della scansione"""
        while not self.scan_finished and not self.abortRequested():
            self.waitForAbort(0.5)  # controlla ogni 500ms
        return not self.abortRequested()

    def reset(self):
        """Reset per la prossima scansione"""
        self.scan_finished = False


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def execute_addon_with_builtin(addon_id, params=None):
    builtin_cmd = f'RunAddon({addon_id},{params})' if params else f'RunAddon({addon_id})'
    xbmc.executebuiltin(builtin_cmd, True)


def get_paths_for_init(db_params):
    query = '''
            WITH matched_sources AS (SELECT s.strMultipath
                                     FROM source s
                                              JOIN path p ON s.strMultipath = p.strPath),

                 orphan_paths AS (SELECT DISTINCT p.strPath
                                  FROM path p
                                           JOIN source s ON INSTR(p.strPath, s.strMultipath) = 1
                                  WHERE s.strMultipath NOT IN (SELECT strMultipath FROM matched_sources)
                                    AND p.strPath NOT IN (SELECT strMultipath FROM matched_sources)),

                 ranked AS (SELECT p.strPath, 1 AS priority
                            FROM source s
                                     JOIN path p ON s.strMultipath = p.strPath

                            UNION ALL

                            SELECT o.strPath, 2 AS priority
                            FROM orphan_paths o
                            WHERE NOT EXISTS (SELECT 1
                                              FROM orphan_paths ancestor
                                              WHERE INSTR(o.strPath, ancestor.strPath) = 1
                                                AND ancestor.strPath != o.strPath))

            SELECT strPath
            FROM ranked
            ORDER BY priority, strPath'''
    use_webdav = db_params.get('sourcetype') == 'webdav'
    results = db_scan.execute_from_db_gateway(db_params, '/kodi-clients/library-installer-paths', 'GET')
    central_results = [result.get('strPath') for result in results]
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.row_factory = sqlite3.Row
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    music_db_cursor.execute(query)
    local_results = music_db_cursor.fetchall()
    music_db_cursor.close()
    music_db.close()
    local_results = {result['strPath'] for result in local_results}
    paths_to_scan = []
    for path in central_results:
        path_to_check = path if not use_webdav else db_scan.convert_from_smb_to_davs(path)
        if path_to_check not in local_results:
            paths_to_scan.append(path)
    return paths_to_scan


def init_library():
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)
    db_params = db_scan.get_db_params()
    paths_to_scan = get_paths_for_init(db_params)
    if paths_to_scan:
        monitor = ScanMonitor()
        for path_to_scan in paths_to_scan:
            monitor.reset()
            params = db_scan.encode_string(f'?path={path_to_scan};mode=scan', safe_chars='()!')
            execute_addon_with_builtin('script.scanner.trigger', params)
            if monitor.wait_for_scan():
                xbmc.log(f"Inizializzazione completata per: {path_to_scan}", xbmc.LOGINFO)


if __name__ == "__main__":
    init_library()
