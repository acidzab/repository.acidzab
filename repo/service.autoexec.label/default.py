import json
import re
import sqlite3
import time
from datetime import datetime, timedelta
from urllib.parse import unquote, quote

import pymysql
import xbmc
import db_scan
import xbmcaddon
import xbmcgui


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


# Workaround per il bug di datetime.strptime in Kodi
def safe_strptime(date_string, format_string):
    try:
        return datetime.strptime(date_string, format_string)
    except (TypeError, AttributeError):
        # Fallback usando time.strptime quando datetime.strptime è corrotto
        return datetime.fromtimestamp(time.mktime(time.strptime(date_string, format_string)))


def get_album_paths_by_id_album(albums, db_params):
    path_by_id_album = {}
    smb_source_base = db_params.get('sambasource')
    webdav_source_base = db_params.get('webdavsource')
    for id_album in albums:
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
            if common_prefix != f'{webdav_source_base}/' and common_prefix != f'{smb_source_base}/':
                path_by_id_album[id_album] = common_prefix
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


def get_album_infos(use_central, db_params, music_db_name):
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
            album_paths.append(result['strPath'] if not use_webdav or use_central else db_scan.convert_from_davs_to_smb(result['strPath']))
            paths_by_id_album[result['idAlbum']] = album_paths
        album_path_by_id = get_album_paths_by_id_album(paths_by_id_album, db_params)
        for result in query_results:
            album_info = {'mbid': result['strMusicBrainzAlbumID'], 'path': album_path_by_id.get(result['idAlbum'])}
            album_infos.append(album_info)
    return album_infos


def get_releases_to_align(db_params, music_db_name):
    central_albums = get_album_infos(True, db_params, music_db_name)
    local_albums = get_album_infos(False, db_params, music_db_name)
    # Converti in set per confronti più efficienti
    central_set = {(album['mbid'], album['path']) for album in central_albums}
    local_set = {(album['mbid'], album['path']) for album in local_albums}

    # Trova differenze
    to_add_keys = central_set - local_set
    to_remove_keys = local_set - central_set

    # Ricostruisci gli oggetti album
    albums_to_add = [{'mbid': mbid, 'path': path} for mbid, path in to_add_keys]
    albums_to_remove = [{'mbid': mbid, 'path': path} for mbid, path in to_remove_keys]

    # Raccogli tutti i paths da scansionare
    paths_to_scan = [album['path'] for album in albums_to_add + albums_to_remove]

    message = f'Mancano i seguenti album {albums_to_add}'
    log(message)
    message = f'I seguenti album sono da rimuovere nel db locale {albums_to_remove}'
    log(message)
    return paths_to_scan


def init_music_database():
    db_params = db_scan.get_db_params()
    db_versions = db_scan.get_latest_kodi_dbs()
    music_db_name = db_versions.get('MyMusic')
    paths_to_scan = sync_paths_to_scan(db_params, music_db_name)
    params = '?mode=init'
    if paths_to_scan and not xbmc.getCondVisibility('Library.IsScanningMusic'):
        query_string = ';'.join([f"path={path}" for path in paths_to_scan if path])
        params = db_scan.encode_string(f'?{query_string};mode=init', safe_chars='()!')
    execute_addon_with_builtin('script.scanner.trigger', params)


def get_albums_to_sync(dt_last_scanned_local, music_db_name, db_params):
    central_query = '''
                    SELECT songview.strPath,
                           album.dateAdded,
                           album.idAlbum
                    FROM album
                             JOIN songview ON songview.idAlbum = album.idAlbum
                    WHERE album.dateAdded BETWEEN %s AND %s
                    GROUP BY songview.strPath
                    ORDER BY album.dateAdded'''
    query_local = '''
                  SELECT songview.strPath, album.dateAdded, album.idAlbum
                  FROM album
                           JOIN songview ON songview.idAlbum = album.idAlbum
                  WHERE songview.strPath IN (%s)
                  GROUP BY songview.strPath'''
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
            central_cursor.execute(central_query, (from_date_str_local, to_date_str))
            log(central_cursor.mogrify(central_query, (from_date_str_local, to_date_str)))
            central_results.extend(central_cursor.fetchall())

    central_dt_added_by_path = {}
    local_paths_to_check = []
    if central_results:
        paths_by_id_album = {}
        for result in central_results:
            album_paths = paths_by_id_album.get(result.get('idAlbum'))
            if not album_paths:
                album_paths = []
            album_paths.append(result.get('strPath'))
            local_paths_to_check.append(db_scan.convert_from_smb_to_davs(result.get('strPath')) if use_webdav else result.get('strPath'))
            paths_by_id_album[result.get('idAlbum')] = album_paths
        album_path_by_id = get_album_paths_by_id_album(paths_by_id_album, db_params)
        central_dt_added_by_path = {album_path_by_id.get(result.get('idAlbum')): result.get('dateAdded') for result in central_results}
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.row_factory = sqlite3.Row
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    chunks = [local_paths_to_check[i:i + 999] for i in range(0, len(local_paths_to_check), 999)]
    for chunk in chunks:
        placeholders = ','.join(['?'] * len(chunk))
        music_db_cursor.execute(query_local % placeholders, chunk)
        local_results.extend(music_db_cursor.fetchall())
    music_db_cursor.close()
    music_db.close()

    local_dt_added_by_path = {}
    if local_results:
        paths_by_id_album = {}
        for result in local_results:
            album_paths = paths_by_id_album.get(result['idAlbum'])
            if not album_paths:
                album_paths = []
            album_paths.append(result['strPath'] if not use_webdav else db_scan.convert_from_davs_to_smb(result['strPath']))
            paths_by_id_album[result['idAlbum']] = album_paths
        album_path_by_id = get_album_paths_by_id_album(paths_by_id_album, db_params)
        local_dt_added_by_path = {album_path_by_id.get(result['idAlbum']): result['dateAdded'] for result in local_results}

    for central_path in central_dt_added_by_path.keys():
        local_dt_added = local_dt_added_by_path.get(central_path)
        central_dt_added = central_dt_added_by_path.get(central_path)
        if local_dt_added and local_dt_added != central_dt_added:
            albums_to_sync.append(central_path)
    return albums_to_sync


def sync_paths_to_scan(db_params, music_db_name):
    local_props = get_properties(False, db_params)
    local_last_scanned = local_props.get('librarylastupdated')
    albums_to_sync = get_albums_to_sync(local_last_scanned, music_db_name, db_params)
    albums_to_align = get_releases_to_align(db_params, music_db_name)
    paths_to_scan = set()
    paths_to_scan.update(albums_to_sync)
    paths_to_scan.update(albums_to_align)
    return paths_to_scan


def sync_library():
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)
    db_params = db_scan.get_db_params()
    if db_params.get('table'):
        db_scan.get_jsons_to_process(db_params)
    exec_mode = 'scan'
    music_db_name = db_scan.get_latest_kodi_dbs().get('MyMusic')
    current_scans = sync_paths_to_scan(db_params, music_db_name)
    if current_scans and not xbmc.getCondVisibility('Library.IsScanningMusic'):
        query_string = ';'.join([f"path={path}" for path in current_scans if path])
        params = db_scan.encode_string(f'?{query_string};mode={exec_mode}', safe_chars='()!')
        execute_addon_with_builtin('script.scanner.trigger', params)
        dialog = xbmcgui.Dialog()
        icon_path = xbmcaddon.Addon().getAddonInfo('path') + '/' + 'icon.png'
        dialog.notification(addon_name, 'Sincronizzazione della libreria completata', icon_path)


if __name__ == "__main__":
    sync_library()
