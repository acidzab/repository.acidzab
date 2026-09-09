import json
import os
import re
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote, unquote

import db_scan
import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

audio_extensions = ['.mp3', '.wav', '.wma', '.aac', '.flac', '.ogg', '.m4a', '.aiff', '.aif', '.alac', '.amr', '.ape',
                    '.au', '.mpc', '.tta', '.wv', '.opus']
addon_name = xbmcaddon.Addon().getAddonInfo('name')
addon_id = xbmcaddon.Addon().getAddonInfo('id')
sqlite_params_limit = 999
# Semaforo per non sovraccaricare Kodi con troppe richieste simultanee
_texture_semaphore = threading.Semaphore(4)
confluence_skin_dir = 'skin.confluence.zabarchives'


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def natural_key(s):
    """Chiave di ordinamento naturale: 'thumb2' < 'thumb10'"""
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]


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
    return sources


def get_id_albums(paths):
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    chunks = [paths[i:i + sqlite_params_limit] for i in range(0, len(paths), sqlite_params_limit)]
    results = []
    for chunk in chunks:
        placeholders = ' OR '.join(['vsong.strPath LIKE ?||\'%\''] * len(chunk))
        query = '''
                SELECT DISTINCT vsong.idAlbum
                FROM songview vsong
                WHERE %s
                ''' % placeholders
        results = music_db_cursor.execute(query, chunk).fetchall()
    music_db_cursor.close()
    music_db.close()
    album_ids = [idAlbum for (idAlbum,) in results]
    return album_ids


def get_view_paths(paths):
    view_paths = set()
    query = "SELECT path FROM VIEW vista WHERE vista.path IN (%s)"
    view_mode_db_path = db_scan.get_view_modes_db_path()
    view_mode_db = sqlite3.connect(view_mode_db_path)
    view_mode_db.set_trace_callback(log)
    view_mode_db_cursor = view_mode_db.cursor()
    chunks = [paths[i:i + sqlite_params_limit] for i in range(0, len(paths), sqlite_params_limit)]
    results = []
    for chunk in chunks:
        placeholders = ','.join(['?'] * len(chunk))
        results.extend(view_mode_db_cursor.execute(query % placeholders, chunk).fetchall())
    view_mode_db_cursor.close()
    view_mode_db.close()
    for (path,) in results:
        view_paths.add(path)
    return view_paths


def get_ids_to_refresh(paths_from_params, use_webdav):
    paths = []
    if paths_from_params:
        for path in paths_from_params:
            if use_webdav and not path.startswith('dav'):
                path = db_scan.convert_from_smb_to_davs(path)
            if path not in paths:
                paths.append(path)
    id_albums = get_id_albums(paths)
    return id_albums


def get_scanned_albums_paths(id_albums, exec_mode):
    results = []
    query = '''
            SELECT DISTINCT path.strPath
            FROM song
                     JOIN path path ON song.idPath = path.idPath
            WHERE song.idAlbum IN (%s)'''
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    if exec_mode == 'init':
        id_album_subquery = 'SELECT idAlbum FROM album'
        results.extend(music_db_cursor.execute(query % id_album_subquery).fetchall())
    elif id_albums:
        chunks = [id_albums[i:i + sqlite_params_limit] for i in range(0, len(id_albums), sqlite_params_limit)]
        for chunk in chunks:
            placeholders = ','.join(['?'] * len(chunk))
            results.extend(music_db_cursor.execute(query % placeholders, chunk).fetchall())
    music_db_cursor.close()
    music_db.close()
    album_paths = [strPath for (strPath,) in results if strPath is not None]
    return album_paths


def add_new_view_record(directory, view_mode, sort_method, exsisting_paths, skin_dir):
    directory_to_add = directory not in exsisting_paths
    if directory_to_add:
        view_mode_db_path = db_scan.get_view_modes_db_path()
        view_mode_db = sqlite3.connect(view_mode_db_path)
        view_mode_db.set_trace_callback(log)
        view_mode_db_cursor = view_mode_db.cursor()
        # inserisco il record sul db delle view mode
        insert_query = "INSERT INTO view (window, path, viewMode, sortMethod, sortOrder, sortAttributes, skin) VALUES (?,?,?,?,?,?,?)"
        insert_values = (10502, directory, view_mode, sort_method, 1, 0, skin_dir,)
        view_mode_db_cursor.execute(insert_query, insert_values)
        view_mode_db.commit()
        view_mode_db_cursor.close()
        view_mode_db.close()


def force_confluence_wall_view_for_files(directory, exsisting_paths):
    add_new_view_record(directory, 66036, 1, exsisting_paths, confluence_skin_dir)


def get_paths_to_convert(albums_by_source):
    paths_to_convert = []
    for source in albums_by_source.keys():
        if source not in paths_to_convert:
            paths_to_convert.append(source)
        album_paths = albums_by_source.get(source)
        for album_path in album_paths:
            # calcolo i path
            # step 1: rimuovo la sorgente dal path
            path_without_source = album_path.replace(source, '')
            # step 2: splitto il path filtrato, con lo strip per togliere l'ultimo elemento vuoto a causa dello / finale
            splitted_path = [path for path in path_without_source.split('/') if path.strip()]
            # step 3: se il path splittato è maggiore di 1, vuol dire che la cartella finale è una sottocartella
            if len(splitted_path) > 1:
                splitted_path.pop()
                constructed_path = f'{source}'
                for split in splitted_path:
                    constructed_path = constructed_path + split + '/'
                    if constructed_path not in paths_to_convert:
                        paths_to_convert.append(constructed_path)
    return paths_to_convert


def update_texture_path(dir_path, img_vfs_url):
    texture_db_path = db_scan.get_textures_db_path()
    texture_db = sqlite3.connect(texture_db_path)
    texture_db.set_trace_callback(log)
    texture_db_cursor = texture_db.cursor()
    find_id_query = "select id, texture from path where url = ? and type = 'thumb'"
    update_query = "update path set texture= ? where id = ?"
    texture_path_result = texture_db_cursor.execute(find_id_query, (dir_path,)).fetchone()
    if not texture_path_result:
        insert_query = "insert into path (url, type, texture) values(?, ?, ?)"
        texture_db_cursor.execute(insert_query, (dir_path, 'thumb', img_vfs_url))
        texture_db.commit()
    elif texture_path_result[1] != img_vfs_url:
        id_texture_path = texture_path_result[0]
        texture_db_cursor.execute(update_query, (img_vfs_url, id_texture_path,))
        texture_db.commit()
    texture_db_cursor.close()
    texture_db.close()


def clean_texture_path():
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    query = "SELECT strPath FROM path"
    results = music_db_cursor.execute(query).fetchall()
    music_db_cursor.close()
    music_db.close()
    valid_paths = set([strPath for (strPath,) in results if strPath is not None])
    texture_db_path = db_scan.get_textures_db_path()
    texture_db = sqlite3.connect(texture_db_path)
    texture_db.set_trace_callback(log)
    texture_db_cursor = texture_db.cursor()
    textures_paths_query = "SELECT url FROM path"
    textures_results = texture_db_cursor.execute(textures_paths_query).fetchall()
    textures_paths = set([url for (url,) in textures_results if url is not None])
    paths_to_remove = [(texture_path,) for texture_path in textures_paths if texture_path not in valid_paths]
    if paths_to_remove:
        delete_query = "DELETE FROM path WHERE url=? and type='thumb'"
        texture_db_cursor.executemany(delete_query, paths_to_remove)
        texture_db.commit()
    texture_db_cursor.close()
    texture_db.close()


def convert_to_thumb_view(paths_to_convert, use_webdav, id_albums, exec_mode, sources):
    if paths_to_convert:
        progress = xbmcgui.DialogProgressBG()
        total_dirs_to_process = len(paths_to_convert)
        progress.create(addon_name, message='Imposto la vista di default per i file')
        try:
            exsisting_paths = get_view_paths(paths_to_convert)
            for (step, directory) in enumerate(paths_to_convert, 1):
                force_confluence_wall_view_for_files(directory, exsisting_paths)
                percentuale = (step / total_dirs_to_process) * 100
                progress.update(message=directory, percent=int(percentuale))
        finally:
            progress.close()
        progress.create(addon_name, message='Precarico le miniature sui file')
        try:
            paths_by_id_album = get_album_paths_by_id(id_albums, exec_mode == 'init')
            paths_to_cache = get_thumbs_to_cache(id_albums, exec_mode, paths_by_id_album, use_webdav, sources)
            cache_thumbs(paths_to_cache, progress)
            clean_texture_path()
        finally:
            progress.close()


def get_kodi_image_path(art_url):
    # decodifico il file path con il path image per Kodi per triggerare il job di cache
    # Kodi goes lowercase and doesn't encode some chars
    texture_url = 'image://{0}/'.format(quote(art_url, '()!'))
    texture_url = re.sub(r'%[0-9A-F]{2}', lambda mo: mo.group().lower(), texture_url)
    # Sostituisci manualmente il carattere `~` con la sua codifica
    texture_url = texture_url.replace('~', '%7e')
    return texture_url


# ottengo gli art album da esporre quando si consultano le cartelle dalla vista per sorgenti (File su Kodi)
def get_thumbs_to_cache(id_albums, exec_mode,
                        paths_by_id_album, use_webdav, sources):
    thumbs_to_cache = {}
    translated_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(translated_path)
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    query = '''
            SELECT album.idAlbum, art.type AS artType, art.url
            FROM art
                     JOIN album on album.idAlbum = art.media_id
            WHERE art.media_id IN (%s)
              AND art.media_type = 'album'
            '''
    info_album_query = '''
                       SELECT iTrack >> 16 AS iDisc, albumview.idAlbum, songview.strPath
                       FROM albumview
                           JOIN songview
                       ON songview.idAlbum = albumview.idAlbum
                       WHERE albumview.idAlbum IN (%s)
                       GROUP BY albumview.idAlbum,
                           iDisc
                       ORDER BY albumview.idAlbum,
                           iDisc
                       '''
    results = []
    info_album_results = []
    if exec_mode == 'init':
        id_albums_subquery = 'SELECT idAlbum FROM album'
        results.extend(music_db_cursor.execute(query % id_albums_subquery).fetchall())
        info_album_results.extend(music_db_cursor.execute(info_album_query % id_albums_subquery).fetchall())
    elif id_albums:
        chunks = [id_albums[i:i + 999] for i in range(0, len(id_albums), 999)]
        for chunk in chunks:
            placeholders = ','.join(['?'] * len(chunk))
            results.extend(music_db_cursor.execute(query % placeholders, chunk).fetchall())
            info_album_results.extend(music_db_cursor.execute(info_album_query % placeholders, chunk).fetchall())
    music_db_cursor.close()
    music_db.close()
    if results:
        arts_by_id_album = {}
        for (idAlbum, artType, url) in results:
            arts = arts_by_id_album.get(idAlbum)
            if not arts:
                arts = {}
            arts[artType] = url
            arts_by_id_album[idAlbum] = arts
        infos_album_by_path = {}
        for (iDisc, idAlbum, strPath) in info_album_results:
            infos_album = infos_album_by_path.get(strPath)
            if not infos_album:
                infos_album = []
            infos_album.append(iDisc)
            infos_album_by_path[strPath] = infos_album
        for id_album in arts_by_id_album.keys():
            paths = paths_by_id_album.get(id_album)
            arts = arts_by_id_album.get(id_album)
            common_path = get_album_common_path(paths, sources)
            paths_to_check = []
            paths_to_check.append(common_path)
            if common_path not in paths:
                paths_to_check.extend(paths)
            for path in paths_to_check:
                infos_album = infos_album_by_path.get(path)
                if infos_album and len(infos_album) == 1 and not len(paths_to_check) <= 1:
                    art_type = f'thumb{infos_album[0]}'
                else:
                    art_type = 'thumb'
                url = arts.get(art_type)
                encoded_image = get_kodi_image_path(url)
                message = f'{path}' if not use_webdav else f'{unquote(path)}'
                thumbs_to_cache[encoded_image] = (message, path)
    return thumbs_to_cache


def get_album_common_path(song_paths, sources):
    if not song_paths:
        return None

    common_prefix = os.path.commonprefix(song_paths)

    # commonprefix può tagliare a metà un nome, tronchiamo all'ultimo slash
    if '/' in common_prefix:
        common_prefix = common_prefix[:common_prefix.rfind('/') + 1]
    else:
        return None

    if any(common_prefix.startswith(source) and common_prefix != source for source in sources):
        return common_prefix

    return None


def get_album_paths_by_id(id_albums, fetch_all_albums):
    music_db_path = db_scan.get_music_db_path()
    query = '''
            SELECT DISTINCT song.idAlbum,
                            strPath,
                            song.idPath
            FROM song
                     JOIN path ON song.idPath = path.idPath
            WHERE song.idAlbum IN (%s)
              AND (SELECT COUNT(DISTINCT (idAlbum))
                   FROM song AS song2
                   WHERE idPath = song.idPath) = 1
            ORDER BY strPath ASC'''
    id_albums_subquery = 'SELECT idAlbum FROM album'
    query_results = []
    music_db = sqlite3.connect(music_db_path)
    music_db.row_factory = sqlite3.Row
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    if fetch_all_albums:
        music_db_cursor.execute(query % id_albums_subquery)
        query_results.extend(music_db_cursor.fetchall())
    elif id_albums:
        chunks = [id_albums[i:i + 999] for i in range(0, len(id_albums), 999)]
        for chunk in chunks:
            placeholders = ','.join(['?'] * len(chunk))
            music_db_cursor.execute(query % placeholders, chunk)
            query_results.extend(music_db_cursor.fetchall())
    music_db_cursor.close()
    music_db.close()
    paths_by_album = {}
    if query_results:
        for result in query_results:
            paths = paths_by_album.get(result['idAlbum'])
            if not paths:
                paths = list()
            path = result['strPath']
            paths.append(path)
            paths_by_album[result['idAlbum']] = paths
    return paths_by_album


def _cache_single_thumb(path_to_cache, dir_path, img_vfs_url):
    """Cacha una singola texture in modo thread-safe"""
    with _texture_semaphore:
        try:
            update_texture_path(dir_path, img_vfs_url)
            with xbmcvfs.File(path_to_cache):
                pass
            return path_to_cache, True
        except Exception as e:
            log(f'Errore caching texture {path_to_cache}: {e}')
            return path_to_cache, False


def cache_thumbs(paths_to_cache, progress_bar):
    total = len(paths_to_cache)
    completed = 0
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(
                _cache_single_thumb,
                path,
                paths_to_cache[path][1],  # dir_path
                path  # img_vfs_url
            ): path
            for path in paths_to_cache
        }

        for future in as_completed(futures):
            path, success = future.result()
            message = paths_to_cache[path][0]

            with lock:
                completed += 1
                percent = int((completed / total) * 100)
                progress_bar.update(message=message, percent=percent)


def convert_playlists_to_info_media_view():
    progress = xbmcgui.DialogProgressBG()
    playlists = xbmcvfs.listdir(os.path.join('special://profile/playlists/music'))[1]
    playlists_paths = [f'special://profile/playlists/music/{playlist}/' for playlist in playlists]
    exsisting_playlists = get_view_paths(playlists_paths)
    progress.create(addon_name, message='Imposto la vista di default per le playlist')
    try:
        for (step, playlist) in enumerate(playlists, 1):
            playlist_path = f'special://profile/playlists/music/{playlist}/'
            add_new_view_record(playlist_path, 66042, 22, exsisting_playlists, confluence_skin_dir)
            percentuale = (step / len(playlists)) * 100
            progress.update(message=playlist, percent=int(percentuale))
    finally:
        progress.close()


def switch_to_thumb_view_for_files():
    db_params = db_scan.get_db_params()
    paths_from_params = db_scan.get_paths_from_params()
    exec_mode = db_scan.get_exec_mode()
    use_webdav = db_params.get('sourcetype') == 'webdav'
    sources = get_sources()
    sources_paths = [source.get('file') for source in sources]
    id_albums = []
    ## exec mode non valorizzato -> lancio secco dagli addon
    if exec_mode and exec_mode != 'init':
        id_albums = get_ids_to_refresh(paths_from_params, use_webdav)
    elif not exec_mode:
        exec_mode = 'init'
    album_paths = get_scanned_albums_paths(id_albums, exec_mode)
    albums_by_source = {}
    for source_path in sources_paths:
        source_into_album = [album_path for album_path in album_paths if source_path in album_path]
        if source_into_album:
            albums_by_source[source_path] = source_into_album
    paths_to_convert = get_paths_to_convert(albums_by_source)
    convert_to_thumb_view(paths_to_convert, use_webdav, id_albums, exec_mode, sources_paths)
    convert_playlists_to_info_media_view()
    builtin_cmd = f'NotifyAll({addon_id}, OnViewSwitched)'
    xbmc.executebuiltin(builtin_cmd)


if __name__ == '__main__':
    log(addon_name)
    switch_to_thumb_view_for_files()
