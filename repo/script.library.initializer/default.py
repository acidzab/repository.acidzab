import sqlite3

import db_scan
import pymysql
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


def get_paths_to_scan(paths):
    """
    Raggruppa i path per prefisso comune.

    Args:
        paths: lista di path da raggruppare

    Returns:
        dict: mappa {path_comune: [lista_sottopath]}
    """
    path_groups = {}
    processed = set()

    for path in paths:
        if path in processed:
            continue

        # Trova tutti i path che condividono lo stesso prefisso
        related_paths = [p for p in paths if p.startswith(path) or path.startswith(p)]

        if not related_paths:
            continue

        # Trova il prefisso comune tra i path correlati
        common_prefix = related_paths[0]
        min_length = min(len(p) for p in related_paths)

        for i in range(min_length):
            char = related_paths[0][i]
            if all(p[i] == char for p in related_paths):
                common_prefix = related_paths[0][:i + 1]
            else:
                break

        # Tronca all'ultimo slash
        if common_prefix and '/' in common_prefix:
            last_slash = common_prefix.rfind('/')
            common_prefix = common_prefix[:last_slash + 1]

        # Aggiungi al gruppo solo se ci sono sottopath
        subpaths = [p for p in related_paths if p != common_prefix]
        if subpaths or len(related_paths) == 1:
            path_groups[common_prefix] = related_paths
            processed.update(related_paths)

    return path_groups


def get_paths_for_init(db_params):
    results = []
    query = '''
            SELECT p.strPath
            FROM source s
                     JOIN path p ON s.strMultipath = p.strPath
            UNION
            SELECT DISTINCT path.strPath
            FROM path
                     JOIN source ON path.strPath LIKE %s
            WHERE source.strMultipath NOT IN
                  (SELECT s2.strMultipath
                   FROM source s2
                            JOIN path p2 ON s2.strMultipath = p2.strPath)
              AND path.strPath != source.strMultipath
            ORDER BY strPath'''
    music_db_name = db_scan.get_latest_kodi_dbs().get('MyMusic')
    host = db_params.get('host')
    username = db_params.get('user')
    password = db_params.get('pass')
    use_webdav = db_params.get('sourcetype') == 'webdav'
    central_db = pymysql.connect(host=host, user=username, password=password, database=music_db_name, port=3306,
                                 cursorclass=pymysql.cursors.DictCursor, connect_timeout=18000)
    with central_db:
        with central_db.cursor() as central_cursor:
            like_operator = '''CONCAT(source.strMultipath, '%')'''
            central_cursor.execute(query, (like_operator,))
            log(central_cursor.mogrify(query, (like_operator,)))
            results = central_cursor.fetchall()

    results = [result.get('strPath') for result in results]
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.row_factory = sqlite3.Row
    music_db.set_trace_callback(log)
    like_operator = '''source.strMultipath || '%' '''
    music_db_cursor = music_db.cursor()
    music_db_cursor.execute(query % '?', (like_operator,))
    local_results = music_db_cursor.fetchall()
    music_db_cursor.close()
    music_db.close()
    local_results = [result['strPath'] for result in local_results]
    paths_to_scan = []
    for path in results:
        path_to_check = path if not use_webdav else db_scan.convert_from_smb_to_davs(path)
        if path_to_check not in local_results:
            paths_to_scan.append(path)
    paths_to_scan_filtered = get_paths_to_scan(paths_to_scan)
    return list(paths_to_scan_filtered.keys())


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
