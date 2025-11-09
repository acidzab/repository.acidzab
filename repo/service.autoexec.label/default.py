import json
import os.path
import sqlite3
import time
from datetime import datetime, timedelta

import db_scan
import pymysql
import requests
import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

addon_name = xbmcaddon.Addon().getAddonInfo('name')


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


# Workaround per il bug di datetime.strptime in Kodi
def safe_strptime(date_string, format_string):
    try:
        return datetime.strptime(date_string, format_string)
    except (TypeError, AttributeError):
        # Fallback usando time.strptime quando datetime.strptime è corrotto
        return datetime.fromtimestamp(time.mktime(time.strptime(date_string, format_string)))


def get_sources():
    json_payload = {
        "jsonrpc": "2.0",
        "method": "AudioLibrary.GetSources",
        "id": "1",
        "params": {
            "properties": [
                "file"
            ]
        }
    }
    sources = []
    get_sources_req = xbmc.executeJSONRPC(json.dumps(json_payload))
    response = json.loads(get_sources_req)
    if response.get('result'):
        sources = response.get('result').get('sources')
    return [source.get('file') for source in sources]


def get_central_playlists(smb_path, db_params):
    json_get_directory_payload = {
        "jsonrpc": "2.0",
        "method": "Files.GetDirectory",
        "id": "1",
        "params": {
            "directory": smb_path
        }
    }
    return db_scan.execute_from_central_kodi_webserver(db_params, json_get_directory_payload).get('result')


# controllo che sia una directory valida guardando che sia presente dentro i path sul db e che non sia una sorgente
def filter_commons_dict(albums_dict, commons_path_dict, sources):
    query = 'SELECT strPath FROM path'
    query_results = []
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.row_factory = sqlite3.Row
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    music_db_cursor.execute(query)
    query_results.extend(music_db_cursor.fetchall())
    music_db_cursor.close()
    music_db.close()
    paths = []
    for (strPath,) in query_results:
        paths.append(strPath)
    for id_album in commons_path_dict:
        album_paths = commons_path_dict.get(id_album)
        filtered_paths = []
        for path in album_paths:
            if path in paths and path not in sources:
                filtered_paths.append(path)
        if len(filtered_paths) == 0:
            filtered_paths.extend(albums_dict.get(id_album))
        commons_path_dict[id_album] = filtered_paths


def get_album_paths_by_id_album(albums, sources):
    path_by_id_album = {}
    for id_album in albums:
        album_paths = []
        paths = albums.get(id_album)
        if paths:
            common_prefix = ''
            min_length = min(len(path) for path in paths)

            for i in range(min_length):
                char = paths[0][i]
                if all(path[i] == char for path in paths):
                    common_prefix += char
                else:
                    break
            # Trova l'ultimo slash nel prefisso comune
            if common_prefix and '/' in common_prefix:
                last_slash = common_prefix.rfind('/')
                common_prefix = common_prefix[:last_slash + 1]
                album_paths.append(common_prefix)
            path_by_id_album[id_album] = album_paths
    filter_commons_dict(albums, path_by_id_album, sources)
    return path_by_id_album


def execute_addon_with_builtin(addon_id, params=None):
    builtin_cmd = f'RunAddon({addon_id},{params})' if params else f'RunAddon({addon_id})'
    xbmc.executebuiltin(builtin_cmd, True)


def get_properties(call_central, db_params):
    json_payload = {
        "id": "1",
        "jsonrpc": "2.0",
        "method": "AudioLibrary.GetProperties",
        "params": {
            "properties": [
                "librarylastupdated"
            ]
        }
    }
    if call_central:
        props = db_scan.execute_from_central_kodi_webserver(db_params, json_payload).get('result')
    else:
        props = json.loads(xbmc.executeJSONRPC(json.dumps(json_payload, ensure_ascii=False))).get('result')
    return props


def get_album_infos(use_central, db_params, music_db_name, sources):
    use_webdav = db_params.get('sourcetype') == 'webdav'
    music_db_path = db_scan.get_music_db_path()
    album_infos = []
    query = '''
            SELECT DISTINCT album.strMusicBrainzAlbumID, songview.strPath, album.idAlbum
            FROM songview
                     JOIN album ON album.idAlbum = songview.idAlbum
            ORDER BY songview.strPath
            '''
    query_results = []
    if use_central:
        host = db_params.get('host')
        username = db_params.get('user')
        password = db_params.get('pass')
        central_db = pymysql.connect(host=host, user=username, password=password, database=music_db_name, port=3306,
                                     cursorclass=pymysql.cursors.DictCursor, connect_timeout=18000)
        with central_db:
            with central_db.cursor() as central_cursor:
                central_cursor.execute(query)
                log(central_cursor.mogrify(query))
                query_results.extend(central_cursor.fetchall())
    else:
        music_db = sqlite3.connect(music_db_path)
        music_db.row_factory = sqlite3.Row
        music_db.set_trace_callback(log)
        music_db_cursor = music_db.cursor()
        music_db_cursor.execute(query)
        query_results.extend(music_db_cursor.fetchall())
        music_db_cursor.close()
        music_db.close()
    if query_results:
        paths_by_id_album = {}
        for result in query_results:
            album_paths = paths_by_id_album.get(result['idAlbum'])
            if not album_paths:
                album_paths = []
            album_paths.append(result['strPath'] if not use_webdav or use_central else db_scan.convert_from_davs_to_smb(
                result['strPath']))
            paths_by_id_album[result['idAlbum']] = album_paths
        album_path_by_id = get_album_paths_by_id_album(paths_by_id_album, sources)
        for result in query_results:
            album_info = {'mbid': result['strMusicBrainzAlbumID'], 'path': album_path_by_id.get(result['idAlbum'])}
            album_infos.append(album_info)
    return album_infos


def sync_playlists_to_central_path(playlist_source, db_params):
    playlists_response = get_central_playlists(playlist_source, db_params)
    if playlists_response and playlists_response.get('files'):
        playlist_path = xbmcvfs.translatePath('special://profile/playlists/music/')
        use_webdav = db_params.get('sourcetype') == 'webdav'
        for playlist in playlists_response.get('files'):
            central_playlist_path = db_scan.convert_from_smb_to_davs(
                playlist.get('file')) if use_webdav else playlist.get('file')
            local_path = os.path.join(playlist_path, playlist.get('label'))
            xbmcvfs.copy(central_playlist_path, local_path)


def get_releases_to_align(db_params, music_db_name, sources):
    central_albums = get_album_infos(True, db_params, music_db_name, sources)
    local_albums = get_album_infos(False, db_params, music_db_name, sources)
    # Converti in set per confronti più efficienti
    central_set = {(album['mbid'], tuple(album['path'])) for album in central_albums}
    local_set = {(album['mbid'], tuple(album['path'])) for album in local_albums}

    # Trova differenze
    to_add_keys = central_set - local_set
    to_remove_keys = local_set - central_set

    # Ricostruisci gli oggetti album
    albums_to_add = [{'mbid': mbid, 'path': list(path)} for mbid, path in to_add_keys]
    albums_to_remove = [{'mbid': mbid, 'path': list(path)} for mbid, path in to_remove_keys]

    # Raccogli tutti i paths da scansionare
    paths_to_scan = [p for album in albums_to_add + albums_to_remove for p in album['path']]

    message = f'Mancano i seguenti album {albums_to_add}'
    log(message)
    message = f'I seguenti album sono da rimuovere nel db locale {albums_to_remove}'
    log(message)
    return paths_to_scan


def check_for_scans(db_params):
    table = db_params.get('table')
    url = f'{db_params.get('scanserver')}/scans/{table}/status'
    scan_status = requests.get(url)
    scan_status.raise_for_status()
    scan_results = scan_status.json()
    return scan_results and scan_results.get('scan')


def init_music_database():
    db_params = db_scan.get_db_params()
    if db_params.get('table') and check_for_scans(db_params):
        db_scan.reset_scan_status(db_params)
    db_versions = db_scan.get_latest_kodi_dbs()
    music_db_name = db_versions.get('MyMusic')
    paths_to_scan = sync_paths_to_scan(db_params, music_db_name)
    params = '?mode=init'
    if paths_to_scan and not xbmc.getCondVisibility('Library.IsScanningMusic'):
        query_string = ';'.join([f"path={path}" for path in paths_to_scan if path])
        params = db_scan.encode_string(f'?{query_string};mode=init', safe_chars='()!')
    execute_addon_with_builtin('script.scanner.trigger', params)
    monitor = ScanMonitor()
    if monitor.wait_for_scan():
        monitor.reset()
        xbmc.log(f"Sincronizzazione libreria completata", xbmc.LOGINFO)
        emit_final_dialog(addon_name)
        execute_addon_with_builtin('service.scan.checker')


def get_albums_to_sync(dt_last_scanned_local, music_db_name, db_params, sources):
    query = '''
            SELECT songview.strPath,
                   album.dateAdded,
                   album.idAlbum,
                   album.strMusicBrainzAlbumID
            FROM album
                     JOIN songview
                          ON songview.idAlbum = album.idAlbum
            WHERE album.dateAdded BETWEEN %s
                      AND %s
            GROUP BY songview.strPath
            ORDER BY album.dateAdded'''
    from_date_str_local = safe_strptime(dt_last_scanned_local, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
    to_date = datetime.now() + timedelta(days=1)
    to_date_str = to_date.strftime('%Y-%m-%d')
    central_results = []
    local_results = []
    albums_to_sync = []
    host = db_params.get('host')
    username = db_params.get('user')
    password = db_params.get('pass')
    use_webdav = db_params.get('sourcetype') == 'webdav'
    central_db = pymysql.connect(host=host, user=username, password=password, database=music_db_name, port=3306,
                                 cursorclass=pymysql.cursors.DictCursor, connect_timeout=18000)
    with central_db:
        with central_db.cursor() as central_cursor:
            central_cursor.execute(query, (from_date_str_local, to_date_str))
            log(central_cursor.mogrify(query, (from_date_str_local, to_date_str)))
            central_results.extend(central_cursor.fetchall())

    central_dt_added_by_mbid = {}
    if central_results:
        paths_by_id_album = {}
        for result in central_results:
            album_paths = paths_by_id_album.get(result.get('idAlbum'))
            if not album_paths:
                album_paths = []
            album_paths.append(result.get('strPath'))
            paths_by_id_album[result.get('idAlbum')] = album_paths
        album_path_by_id = get_album_paths_by_id_album(paths_by_id_album, sources)
        central_dt_added_by_mbid = {
            result.get('strMusicBrainzAlbumID'): {'paths': album_path_by_id.get(result.get('idAlbum')),
                                                  'dateAdded': result.get('dateAdded')} for result in central_results}
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.row_factory = sqlite3.Row
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    query = query.replace('%s', '?')
    music_db_cursor.execute(query, (from_date_str_local, to_date_str))
    local_results.extend(music_db_cursor.fetchall())
    music_db_cursor.close()
    music_db.close()

    local_dt_added_by_mbid = {}
    if local_results:
        paths_by_id_album = {}
        for result in local_results:
            album_paths = paths_by_id_album.get(result['idAlbum'])
            if not album_paths:
                album_paths = []
            album_paths.append(
                result['strPath'] if not use_webdav else db_scan.convert_from_davs_to_smb(result['strPath']))
            paths_by_id_album[result['idAlbum']] = album_paths
        album_path_by_id = get_album_paths_by_id_album(paths_by_id_album, sources)
        local_dt_added_by_mbid = {
            result['strMusicBrainzAlbumID']: {'paths': album_path_by_id.get(result['idAlbum']),
                                              'dateAdded': result['dateAdded']} for result in local_results}

    for mbid in central_dt_added_by_mbid.keys():
        central_info = central_dt_added_by_mbid.get(mbid)
        local_info = local_dt_added_by_mbid.get(mbid)
        local_dt_added = local_info.get('dateAdded') if local_info else None
        central_dt_added = central_info.get('dateAdded') if central_info else None
        if central_dt_added and (not local_dt_added or local_dt_added != central_dt_added):
            albums_to_sync.extend(central_info.get('paths'))
    return albums_to_sync


def sync_paths_to_scan(db_params, music_db_name):
    local_props = get_properties(False, db_params)
    local_last_scanned = local_props.get('librarylastupdated')
    sources = get_sources()
    central_playlists_enabled = db_params.get('centralplaylist')
    if central_playlists_enabled:
        playlist_source = f'{db_params.get("sambasource")}/playlists/music/'
        sync_playlists_to_central_path(playlist_source, db_params)
    albums_to_sync = get_albums_to_sync(local_last_scanned, music_db_name, db_params, sources)
    albums_to_align = get_releases_to_align(db_params, music_db_name, sources)
    paths_to_scan = set()
    paths_to_scan.update(albums_to_sync)
    paths_to_scan.update(albums_to_align)
    return paths_to_scan


def emit_final_dialog(addon_name):
    dialog = xbmcgui.Dialog()
    icon_path = xbmcaddon.Addon().getAddonInfo('path') + '/' + 'icon.png'
    dialog.notification(addon_name, 'Sincronizzazione completata', icon_path)


def sync_library():
    log(addon_name)
    db_params = db_scan.get_db_params()
    exec_mode = 'scan'
    music_db_name = db_scan.get_latest_kodi_dbs().get('MyMusic')
    current_scans = sync_paths_to_scan(db_params, music_db_name)
    if current_scans and not xbmc.getCondVisibility('Library.IsScanningMusic'):
        query_string = ';'.join([f"path={path}" for path in current_scans if path])
        params = db_scan.encode_string(f'?{query_string};mode={exec_mode}', safe_chars='()!')
        execute_addon_with_builtin('script.scanner.trigger', params)
        monitor = ScanMonitor()
        if monitor.wait_for_scan():
            monitor.reset()
            xbmc.log(f"Sincronizzazione libreria completata", xbmc.LOGINFO)
            emit_final_dialog(addon_name)


if __name__ == "__main__":
    sync_library()
