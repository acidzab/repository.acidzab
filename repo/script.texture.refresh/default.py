import json
import os
import re
import sqlite3
from urllib.parse import quote, unquote

import db_scan
import unicodedata
import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

addon_name = xbmcaddon.Addon().getAddonInfo('name')
addon_id = xbmcaddon.Addon().getAddonInfo('id')


class AlignMonitor(xbmc.Monitor):
    def __init__(self):
        super(AlignMonitor, self).__init__()
        self.align_finished = False
        self.file_viewer_done = False
        self.label_preloader_done = False
        self.arts_preloader_done = False

    def onNotification(self, sender, method, data):
        is_label_preloader_installed = xbmc.getCondVisibility('System.HasAddon(script.label.preloader)')
        file_viewer_executed = sender == 'script.file.viewer' and method == 'Other.OnViewSwitched'
        arts_preloader_executed = sender == 'script.music.art.preloader' and method == 'Other.OnArtsPreloaded'
        label_preloader_executed = sender == 'script.label.preloader' and method == 'Other.OnLabelsPreloaded'
        if file_viewer_executed:
            self.file_viewer_done = True
        if arts_preloader_executed:
            self.arts_preloader_done = True
        if label_preloader_executed or not is_label_preloader_installed:
            self.label_preloader_done = True
        if self.file_viewer_done and self.arts_preloader_done and self.label_preloader_done:
            self.align_finished = True

    def wait_for_align(self):
        """Attende il completamento della scansione"""
        while not self.align_finished and not self.abortRequested():
            self.waitForAbort(0.5)  # controlla ogni 500ms
        return not self.abortRequested()

    def reset(self):
        """Reset per la prossima scansione"""
        self.align_finished = False


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def decode_url(path):
    return unquote(path)


def execute_addon_with_builtin(addon_id, params=None):
    builtin_cmd = f'RunAddon({addon_id})'
    if params:
        builtin_cmd = f'RunAddon({addon_id},{params})'
    xbmc.executebuiltin(builtin_cmd, True)


def remove_textures(id_textures):
    textures_requests = []
    for (id_rpc, id_texture) in enumerate(id_textures, 1):
        json_texture_remove_payload = {"jsonrpc": "2.0", "method": "Textures.RemoveTexture", "id": id_rpc,
                                       "params": {"textureid": id_texture}}
        if json_texture_remove_payload not in textures_requests:
            textures_requests.append(json_texture_remove_payload)
    splitted_requests = db_scan.split_json(textures_requests)
    for splitted_request in splitted_requests:
        xbmc.executeJSONRPC(json.dumps(splitted_request))


def get_textures():
    json_get_texture_payload = {
        "jsonrpc": "2.0",
        "method": "Textures.GetTextures",
        "id": "1",
        "params": {
            "properties": [
                "url"
            ],
            "filter": {
                "or": [
                    {
                        "field": "url",
                        "operator": "contains",
                        "value": "image"
                    },
                    {
                        "field": "url",
                        "operator": "contains",
                        "value": "artists"
                    },
                    {
                        "field": "url",
                        "operator": "contains",
                        "value": "etichette"
                    }
                ]
            }
        }
    }
    json_result = xbmc.executeJSONRPC(json.dumps(json_get_texture_payload))
    textures_res = json.loads(json_result).get('result').get('textures')
    textures_urls = []
    for texture in textures_res:
        textures_urls.append(texture.get('url'))
    return textures_urls


def get_textures_id(urls_to_query):
    base_payload = {"jsonrpc": "2.0", "method": "Textures.GetTextures", "id": "1"}
    params = {}
    filter_json = {}
    or_filter_list = []
    for url in urls_to_query:
        or_filter = {
            "field": "url",
            "operator": "is",
            "value": url
        }
        if or_filter not in or_filter_list:
            or_filter_list.append(or_filter)
    textures_id = []
    chunks = [or_filter_list[i:i + 800] for i in range(0, len(or_filter_list), 800)]
    for chunk in chunks:
        filter_json.update({'or': chunk})
        params.update({'filter': filter_json})
        base_payload.update({'params': params})
        json_result = xbmc.executeJSONRPC(json.dumps(base_payload))
        json_result = json.loads(json_result).get('result')
        if json_result:
            textures_id = [texture.get('textureid') for texture in json_result.get('textures')]
    return textures_id


def sanitize(original_string, pattern, replace=""):
    sanitized_string = re.sub(pattern, replace, original_string)
    final_sanitized_string = ''.join(c for c in sanitized_string if unicodedata.category(c)[0] != 'C')
    return final_sanitized_string


def get_label_logo_file_name(label_name):
    label_sanitized = sanitize(label_name, r"\W+").lower().replace(' ', '')
    logo_file_name = f'{label_sanitized}.jpg'
    return logo_file_name


def get_label_logos(textures):
    label_logos = {}
    etichette_base_path = 'special://masterprofile/library/music/etichette/'
    dirs = xbmcvfs.listdir(os.path.join(etichette_base_path))[0]

    for directory in dirs:
        dir_path = f'{etichette_base_path + directory}/'
        files = xbmcvfs.listdir(dir_path)[1]
        for file in files:
            if file.endswith('.jpg') or file.endswith('.png'):
                file_path = f'{etichette_base_path + directory}/{file}'
                # Kodi goes lowercase and doesn't encode some chars
                result = 'image://{0}/'.format(quote(file_path, '()!'))
                result = re.sub(r'%[0-9A-F]{2}', lambda mo: mo.group().lower(), result)
                # Sostituisci manualmente il carattere `~` con la sua codifica
                result = result.replace('~', '%7e')
                if result in textures:
                    label_logos[file] = result
    return label_logos


def get_artists(artists_id):
    artists_payload = {
        "jsonrpc": "2.0",
        "method": "AudioLibrary.GetArtists",
        "id": "1",
        "params": {
            "properties": [
                "thumbnail"
            ]
        }
    }
    json_result = xbmc.executeJSONRPC(json.dumps(artists_payload))
    json_result = json.loads(json_result).get('result')
    artists = json_result.get('artists')
    filtered_artists = list(
        filter(lambda artist: artists_id and artist.get('thumbnail') != '' and artist.get('artistid') in artists_id,
               artists))
    return filtered_artists


def get_songs_by_albums(id_albums):
    song_requests = []
    for (id_rpc, id_album) in enumerate(id_albums, 1):
        json_get_song_payload = {"jsonrpc": "2.0", "method": "AudioLibrary.GetSongs", "id": id_rpc,
                                 "params": {"properties": ["albumid", "artistid", "thumbnail"],
                                            "filter": {"albumid": id_album}}}
        if json_get_song_payload not in song_requests:
            song_requests.append(json_get_song_payload)
    json_result = xbmc.executeJSONRPC(json.dumps(song_requests))
    json_songs = json.loads(json_result)
    songs_by_album = {}
    for json_song in json_songs:
        json_result = json_song.get('result')
        if json_result:
            songs = json_result.get('songs')
            if songs:
                for song in songs:
                    album_songs = songs_by_album.get(song.get('albumid'))
                    if not album_songs:
                        album_songs = []
                    album_songs.append(song)
                    songs_by_album[song.get('albumid')] = album_songs
    return songs_by_album


def get_kodi_image_path(file_path):
    # decodifico il file path con il path image per Kodi per triggerare il job di cache
    # Kodi goes lowercase and doesn't encode some chars
    texture_url = 'image://music@{0}/'.format(quote(file_path, '()!'))
    texture_url = re.sub(r'%[0-9A-F]{2}', lambda mo: mo.group().lower(), texture_url)
    # Sostituisci manualmente il carattere `~` con la sua codifica
    texture_url = texture_url.replace('~', '%7e')
    return texture_url


# ottengo le potenziali texture usate come thumbnail quando si consultano le cartelle dalla vista per sorgenti (File su Kodi)
def get_thumbs_to_refresh_by_id_album(id_albums, textures):
    thumbs_to_refresh = {}
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
              (SELECT idAlbum, strPath || strFilename AS full_path,
                      ROW_NUMBER() OVER (PARTITION BY strPath
                                         ORDER BY decoded_filename COLLATE NOCASE, idSong) AS row_num
               FROM decoded)
            SELECT idAlbum, full_path
            FROM ranked
            WHERE row_num = 1''' % placeholders
        first_tracks_res = music_db_cursor.execute(query, chunk)
        results = first_tracks_res.fetchall()
        for (idAlbum, full_path) in results:
            encoded_image = get_kodi_image_path(full_path)
            if encoded_image in textures:
                thumbs_to_refresh[idAlbum] = encoded_image

    music_db_cursor.close()
    music_db.close()
    return thumbs_to_refresh


def get_albums_by_ids(id_albums):
    album_details_requests = []
    albums = []
    if id_albums:
        for (id_rpc, id_album) in enumerate(id_albums, 1):
            json_album_detail_payload = {"jsonrpc": "2.0", "method": "AudioLibrary.GetAlbumDetails", "id": id_rpc,
                                         "params": {"albumid": id_album,
                                                    "properties": ["art", "thumbnail", "artistid", "albumlabel"]}}
            if json_album_detail_payload not in album_details_requests:
                album_details_requests.append(json_album_detail_payload)
        json_result = xbmc.executeJSONRPC(json.dumps(album_details_requests))
        json_result = json.loads(json_result)
        for single_result in json_result:
            result = single_result.get('result')
            if result:
                album_details = result.get('albumdetails')
                if album_details and album_details not in albums:
                    albums.append(album_details)
    return albums


def get_id_albums_by_paths(id_albums, scanned_path):
    get_directory_payload = {
        "jsonrpc": "2.0",
        "method": "Files.GetDirectory",
        "id": "1",
        "params": {
            "directory": scanned_path,
            "media": "music",
            "properties": [
                "albumid"
            ]
        }
    }
    json_result = json.loads(xbmc.executeJSONRPC(json.dumps(get_directory_payload, ensure_ascii=False))).get('result')
    if json_result:
        for file in json_result.get('files'):
            if file.get('albumid') and file.get('albumid') not in id_albums:
                id_albums.append(file.get('albumid'))
            elif file.get('type') == 'unknown' and file.get('filetype') == 'directory':
                get_id_albums_by_paths(id_albums, file.get('file'))


def build_entity_map(textures, id_albums_added):
    entities_by_type = {}
    id_artists = set()

    # prendo gli album recenti
    last_albums_added = get_albums_by_ids(id_albums_added)

    # prendo le canzoni recenti
    last_songs_added = []

    if id_albums_added:
        songs_by_album = get_songs_by_albums(id_albums_added)
        if songs_by_album:
            for id_album in id_albums_added:
                if songs_by_album.get(id_album):
                    for song in songs_by_album.get(id_album):
                        if song not in last_songs_added:
                            last_songs_added.append(song)

    # processo gli album
    if last_albums_added:
        albums_to_process = []
        for album in last_albums_added:
            id_artists.update(album.get('artistid'))
            thumbnail_url = album.get('thumbnail')
            arts = album.get('art')
            entity = {}
            if thumbnail_url and thumbnail_url not in textures:
                entity = {'id': album.get('albumid'), 'label': album.get('label'), 'thumbnail': thumbnail_url}
            for art_key in arts.keys():
                if art_key.startswith('thumb') and art_key != 'thumbnail':
                    entity[art_key] = arts.get(art_key)
            if entity not in albums_to_process:
                albums_to_process.append(entity)
        entities_by_type['album'] = albums_to_process

    # processo i brani
    if last_songs_added:
        tracks_to_process = []
        for track in last_songs_added:
            id_artists.update(track.get('artistid'))
            thumbnail_url = track.get('thumbnail')
            if thumbnail_url and thumbnail_url in textures:
                entity = {'id': track.get('songid'), 'label': track.get('label'), 'thumbnail': thumbnail_url}
                if entity not in tracks_to_process:
                    tracks_to_process.append(entity)
        entities_by_type['song'] = tracks_to_process

    # gli artisti
    if id_artists:
        artists_to_process = []
        artists = get_artists(id_artists)
        for artist in artists:
            thumbnail_url = artist.get('thumbnail')
            if thumbnail_url in textures:
                entity = {'id': artist.get('artistid'), 'label': artist.get('label'), 'thumbnail': thumbnail_url}
                if entity not in artists_to_process:
                    artists_to_process.append(entity)
        entities_by_type['artist'] = artists_to_process

    # ospite d'eccezione: le label
    label_logos = get_label_logos(textures)
    labels = get_labels_to_refresh(last_albums_added)
    labels_to_process = []
    for label_name in labels.keys():
        logo_file_name = labels.get(label_name)
        logo_texture_path = label_logos.get(logo_file_name)
        if logo_texture_path and logo_texture_path in textures:
            label_entity = {'label': label_name, 'thumbnail': logo_texture_path}
            if label_entity not in labels_to_process:
                labels_to_process.append(label_entity)
    entities_by_type['label'] = labels_to_process

    return entities_by_type


def get_labels_to_refresh(recently_added_albums):
    labels = {}
    if recently_added_albums:
        for recently_added_album in recently_added_albums:
            album_label = recently_added_album.get('albumlabel')
            if album_label and album_label not in labels.keys():
                labels[album_label] = get_label_logo_file_name(album_label)
    return labels


def refresh_textures(paths, exec_mode, paths_from_params):
    textures = get_textures()
    id_albums = []
    for added_path in paths:
        get_id_albums_by_paths(id_albums, added_path)
    entities_by_type = build_entity_map(textures, id_albums)
    file_views_textures_to_refresh = get_thumbs_to_refresh_by_id_album(id_albums, textures)

    progress = xbmcgui.DialogProgressBG()
    progress.create(addon_name)
    total_types_to_process = len(entities_by_type.keys())
    for (step, entity_type) in enumerate(entities_by_type.keys(), 1):
        msg = ''
        if entity_type == 'album':
            msg = 'Processing Albums'
        elif entity_type == 'song':
            msg = 'Processing Songs'
        elif entity_type == 'artist':
            msg = 'Processing Artists'
        elif entity_type == 'label':
            msg = 'Processing Recording Labels'
        progress.update(message=msg)
        entities = entities_by_type.get(entity_type)
        textures_to_refresh = []
        for entity in entities:
            for entity_field in entity.keys():
                if entity_field.startswith('thumb'):
                    thumbnail = entity.get(entity_field)
                    if thumbnail and thumbnail not in textures_to_refresh:
                        textures_to_refresh.append(thumbnail)
        for id_album in id_albums:
            file_texture = file_views_textures_to_refresh.get(id_album)
            if file_texture and file_texture not in textures_to_refresh:
                textures_to_refresh.append(file_texture)
        if textures_to_refresh:
            textures_id = get_textures_id(textures_to_refresh)
            if textures_id:
                remove_textures(textures_id)
        percentuale = (step / total_types_to_process) * 100
        progress.update(percent=int(percentuale))

    progress.close()
    scan_payload = {"jsonrpc": "2.0", "method": "AudioLibrary.Scan", "id": "1",
                    "params": {"directory": "/script.texture.refresh", "showdialogs": False}}
    xbmc.executeJSONRPC(json.dumps(scan_payload))
    query_string = ';'.join([f"path={path}" for path in paths_from_params if path])
    base_params = db_scan.encode_string(f'?{query_string}', safe_chars='()!')
    art_preloader_params = db_scan.encode_string(f'?{query_string};mode={exec_mode}', safe_chars='()!')
    execute_addon_with_builtin("script.file.viewer", base_params)
    execute_addon_with_builtin("script.music.art.preloader", art_preloader_params)
    execute_addon_with_builtin("script.label.preloader", base_params)
    execute_addon_with_builtin("script.alphabetic.library")
    execute_addon_with_builtin("script.genres.preloader")
    monitor = AlignMonitor()
    if monitor.wait_for_align():
        monitor.reset()
        builtin_cmd = f'NotifyAll({addon_id}, OnTextureRefreshed)'
        xbmc.executebuiltin(builtin_cmd)


def execute_texture_refresh():
    db_params = db_scan.get_db_params()
    exec_mode = db_scan.get_exec_mode()
    paths_from_params = db_scan.get_paths_from_params()
    use_webdav = db_params.get('sourcetype') == 'webdav'
    added_paths = []
    if paths_from_params:
        for path in paths_from_params:
            path = db_scan.convert_from_smb_to_davs(path) if use_webdav else path
            if path not in added_paths:
                added_paths.append(path)
    refresh_textures(added_paths, exec_mode, paths_from_params)


if __name__ == '__main__':
    log(addon_name)
    execute_texture_refresh()
