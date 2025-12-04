import json
import os
import re
import sqlite3
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


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


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


def get_scanned_albums_paths(id_albums):
    album_paths = []
    if id_albums:
        music_db_path = db_scan.get_music_db_path()
        music_db = sqlite3.connect(music_db_path)
        music_db.set_trace_callback(log)
        music_db_cursor = music_db.cursor()
        chunks = [id_albums[i:i + sqlite_params_limit] for i in range(0, len(id_albums), sqlite_params_limit)]
        results = []
        for chunk in chunks:
            placeholders = ','.join(['?'] * len(chunk))
            query = '''
            SELECT DISTINCT path.strPath 
            FROM song 
            JOIN path path ON song.idPath = path.idPath 
            WHERE song.idAlbum IN (%s)
            ''' % placeholders
            results.extend(music_db_cursor.execute(query, chunk).fetchall())
        music_db_cursor.close()
        music_db.close()
        album_paths = [strPath for (strPath,) in results if strPath is not None]

    return album_paths


def add_new_view_record(directory, view_mode, sort_method):
    view_mode_db_path = db_scan.get_view_modes_db_path()
    view_mode_db = sqlite3.connect(view_mode_db_path)
    view_mode_db.set_trace_callback(log)
    view_mode_db_cursor = view_mode_db.cursor()
    query_check = "SELECT * FROM VIEW vista WHERE vista.path = {}"
    query_check = query_check.format(f'\"{directory}\"')
    check_res = view_mode_db_cursor.execute(query_check).fetchall()
    skin_dir = xbmc.getSkinDir()
    if not check_res and 'skin.confluence' in skin_dir:
        # inserisco il record sul db delle view mode
        insert_query = "INSERT INTO view (window, path, viewMode, sortMethod, sortOrder, sortAttributes, skin) VALUES (?,?,?,?,?,?,?)"
        insert_values = (10502, directory, view_mode, sort_method, 1, 0, 'skin.confluence',)
        view_mode_db_cursor.execute(insert_query, insert_values)
        view_mode_db.commit()
    view_mode_db_cursor.close()
    view_mode_db.close()


def force_confluence_wall_view_for_files(directory):
    add_new_view_record(directory, 66036, 1)


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
    find_id_query = "select id from path where url = ? and type = 'thumb'"
    update_query = "update path set texture= ? where id = ?"
    id_texture_path_result = texture_db_cursor.execute(find_id_query, (dir_path,)).fetchone()
    if not id_texture_path_result:
        insert_query = "insert into path (url, type, texture) values(?, ?, ?)"
        texture_db_cursor.execute(insert_query, (dir_path, 'thumb', img_vfs_url))
        texture_db.commit()
    else:
        id_texture_path = id_texture_path_result[0]
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


def convert_to_thumb_view(paths_to_convert, use_webdav, id_albums):
    if paths_to_convert:
        progress = xbmcgui.DialogProgressBG()
        textures = get_textures()
        total_dirs_to_process = len(paths_to_convert)
        progress.create(addon_name, message='Imposto la vista di default per i file')
        for (step, directory) in enumerate(paths_to_convert, 1):
            force_confluence_wall_view_for_files(directory)
            percentuale = (step / total_dirs_to_process) * 100
            progress.update(message=directory, percent=int(percentuale))
        progress.close()
        progress.create(addon_name, message='Precarico le miniature sui file')
        paths_to_cache = get_thumbs_to_cache(id_albums, textures, use_webdav)
        cache_thumbs(paths_to_cache, progress)
        clean_texture_path()
        progress.close()


def decode_url(path):
    return unquote(path)


def get_kodi_image_path(file_path):
    # decodifico il file path con il path image per Kodi per triggerare il job di cache
    # Kodi goes lowercase and doesn't encode some chars
    texture_url = 'image://music@{0}/'.format(quote(file_path, '()!'))
    texture_url = re.sub(r'%[0-9A-F]{2}', lambda mo: mo.group().lower(), texture_url)
    # Sostituisci manualmente il carattere `~` con la sua codifica
    texture_url = texture_url.replace('~', '%7e')
    return texture_url


# ottengo le potenziali texture usate come thumbnail quando si consultano le cartelle dalla vista per sorgenti (File su Kodi)
def get_thumbs_to_cache(id_albums, textures, use_webdav):
    thumbs_to_cache = {}
    translated_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(translated_path)
    music_db.create_function('decode', 1, decode_url, deterministic=True)
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    chunks = [id_albums[i:i + 999] for i in range(0, len(id_albums), 999)]
    for chunk in chunks:
        placeholders = ','.join(['?'] * len(chunk))
        query = '''
            WITH decoded AS
              (SELECT strPath,
                      strFilename,
                      decode(strFilename) AS decoded_filename,
                      idSong,
                      idAlbum
               FROM songview
               WHERE idAlbum IN (%s)),
                 ranked AS
              (SELECT strPath || strFilename AS full_path,
                      ROW_NUMBER() OVER (PARTITION BY strPath
                                         ORDER BY decoded_filename COLLATE NOCASE, idSong) AS row_num
               FROM decoded)
            SELECT full_path
            FROM ranked
            WHERE row_num = 1''' % placeholders
        first_tracks_res = music_db_cursor.execute(query, chunk)
        results = first_tracks_res.fetchall()
        for (full_path,) in results:
            encoded_image = get_kodi_image_path(full_path)
            if encoded_image not in textures:
                folder, file = os.path.split(full_path)
                parent_folder = os.path.basename(folder)
                message = f'{parent_folder}/{file}'
                message = unquote(message) if use_webdav else message
                thumbs_to_cache[encoded_image] = (message, f'{full_path.rsplit('/', 1)[0]}/')

    music_db_cursor.close()
    music_db.close()
    return thumbs_to_cache


def cache_thumbs(paths_to_cache, progress_bar):
    total_paths_to_cache = len(paths_to_cache)
    for (step, path_to_cache) in enumerate(paths_to_cache, 1):
        update_texture_path(paths_to_cache.get(path_to_cache)[1], path_to_cache)
        with xbmcvfs.File(path_to_cache):
            pass
        percentuale = (step / total_paths_to_cache) * 100
        message = paths_to_cache.get(path_to_cache)[0]
        progress_bar.update(message=message, percent=int(percentuale))


def get_textures():
    texture_payload = {
        "jsonrpc": "2.0",
        "method": "Textures.GetTextures",
        "id": "1",
        "params": {
            "properties": [
                "url"
            ],
            "filter": {
                "and": [
                    {
                        "field": "url",
                        "operator": "contains",
                        "value": "music@"
                    }
                ]
            }
        }
    }
    json_result = json.loads(xbmc.executeJSONRPC(json.dumps(texture_payload, ensure_ascii=False))).get('result')
    textures = []
    if json_result:
        for texture in json_result.get('textures'):
            if texture.get('url') not in textures:
                textures.append(texture.get('url'))
    return textures


def convert_playlists_to_info_media_view():
    progress = xbmcgui.DialogProgressBG()
    playlists = xbmcvfs.listdir(os.path.join('special://profile/playlists/music'))[1]
    progress.create(addon_name, message='Imposto la vista di default per le playlist')
    for (step, playlist) in enumerate(playlists, 1):
        playlist_path = f'special://profile/playlists/music/{playlist}/'
        add_new_view_record(playlist_path, 66042, 22)
        percentuale = (step / len(playlists)) * 100
        progress.update(message=playlist, percent=int(percentuale))
    progress.close()


def switch_to_thumb_view_for_files():
    db_params = db_scan.get_db_params()
    paths_from_params = db_scan.get_paths_from_params()
    exec_mode = db_scan.get_exec_mode()
    use_webdav = db_params.get('sourcetype') == 'webdav'
    sources = get_sources()
    sources_paths = [source.get('file') for source in sources]
    ## exec mode non valorizzato -> lancio secco dagli addon
    if not exec_mode or exec_mode == 'init':
        id_albums = get_ids_to_refresh(sources_paths, use_webdav)
    else:
        id_albums = get_ids_to_refresh(paths_from_params, use_webdav)
    album_paths = get_scanned_albums_paths(id_albums)
    albums_by_source = {}
    for source_path in sources_paths:
        source_into_album = [album_path for album_path in album_paths if source_path in album_path]
        if source_into_album:
            albums_by_source[source_path] = source_into_album
    paths_to_convert = get_paths_to_convert(albums_by_source)
    convert_to_thumb_view(paths_to_convert, use_webdav, id_albums)
    convert_playlists_to_info_media_view()
    builtin_cmd = f'NotifyAll({addon_id}, OnViewSwitched)'
    xbmc.executebuiltin(builtin_cmd)


if __name__ == '__main__':
    log(addon_name)
    switch_to_thumb_view_for_files()
