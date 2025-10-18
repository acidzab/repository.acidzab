import json

import db_scan
import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

addon_name = xbmcaddon.Addon().getAddonInfo('name')
addon_id = xbmcaddon.Addon().getAddonInfo('id')


class CacheCleanerMonitor(xbmc.Monitor):
    def __init__(self):
        super(CacheCleanerMonitor, self).__init__()
        self.cache_clean_finished = False

    def onNotification(self, sender, method, data):
        if sender == 'script.texture.cache.cleaner' and method == 'Other.OnTextureCacheCleaned':
            self.cache_clean_finished = True

    def wait_for_cache_clean(self):
        """Attende il completamento della scansione"""
        while not self.cache_clean_finished and not self.abortRequested():
            self.waitForAbort(0.5)  # controlla ogni 500ms
        return not self.abortRequested()

    def reset(self):
        """Reset per la prossima scansione"""
        self.cache_clean_finished = False


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def execute_addon_with_rpc(addon_id):
    execute_addon = {
        "jsonrpc": "2.0",
        "method": "Addons.ExecuteAddon",
        "id": "1",
        "params": {
            "addonid": addon_id,
            "wait": True
        }
    }
    json_result = xbmc.executeJSONRPC(json.dumps(execute_addon))
    json_result = json.loads(json_result).get('result')
    return json_result


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
                "and": [
                    {
                        "field": "url",
                        "operator": "startswith",
                        "value": "image://"
                    },
                    {
                        "field": "url",
                        "operator": "doesnotcontain",
                        "value": "addons"
                    },
                    {
                        "field": "url",
                        "operator": "doesnotcontain",
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


def get_all_medias(api_name, media_type, start, end):
    # media_type corrisponde al nome dell'array di risultato quindi: albums, artists, songs
    media_payload = {
        "jsonrpc": "2.0",
        "method": f"AudioLibrary.{api_name}",
        "id": "1",
        "params": {
            "properties": [
                "thumbnail",
                "art"
            ],
            "limits": {
                "start": start,
                "end": end
            }
        }
    }
    json_result = xbmc.executeJSONRPC(json.dumps(media_payload))
    json_result = json.loads(json_result).get('result')
    medias = json_result.get(media_type)
    return medias


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


def get_albums_by_ids(id_albums):
    album_details_requests = []
    albums = []
    if id_albums:
        for (id_rpc, id_album) in enumerate(id_albums, 1):
            json_album_detail_payload = {"jsonrpc": "2.0", "method": "AudioLibrary.GetAlbumDetails", "id": id_rpc,
                                         "params": {"albumid": id_album,
                                                    "properties": ["art", "artistid", "albumlabel", "thumbnail"]}}
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


def build_entity_map_by_entities(artists, albums, songs, textures):
    entities_by_type = {}
    # processo gli album
    if albums:
        albums_to_process = []
        for album in albums:
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
    if songs:
        tracks_to_process = []
        for track in songs:
            thumbnail_url = track.get('thumbnail')
            if thumbnail_url and thumbnail_url not in textures:
                entity = {'id': track.get('songid'), 'label': track.get('label'), 'thumbnail': thumbnail_url}
                if entity not in tracks_to_process:
                    tracks_to_process.append(entity)
        entities_by_type['song'] = tracks_to_process

    # dulcis in fundo gli artisti
    if artists:
        artists_to_process = []
        for artist in artists:
            thumbnail_url = artist.get('thumbnail')
            if thumbnail_url not in textures:
                entity = {'id': artist.get('artistid'), 'label': artist.get('label'), 'thumbnail': thumbnail_url}
                if entity not in artists_to_process:
                    artists_to_process.append(entity)
        entities_by_type['artist'] = artists_to_process

    return entities_by_type


def build_entity_map(id_albums, textures):
    id_artists = set()
    # prendo gli album recenti
    last_albums_added = get_albums_by_ids(id_albums)

    # prendo le canzoni recenti
    last_songs_added = []

    # prendo gli artisti
    artists = []

    if id_albums:
        songs_by_album = get_songs_by_albums(id_albums)
        if songs_by_album:
            for id_album in id_albums:
                if songs_by_album.get(id_album):
                    for song in songs_by_album.get(id_album):
                        if song not in last_songs_added:
                            last_songs_added.append(song)

    if last_albums_added:
        for album in last_albums_added:
            id_artists.update(album.get('artistid'))

    if last_songs_added:
        for track in last_songs_added:
            id_artists.update(track.get('artistid'))

    if id_artists:
        artists = get_artists(id_artists)

    return build_entity_map_by_entities(artists, last_albums_added, last_songs_added, textures)


def cache_medias_textures(entities_by_type):
    progress = xbmcgui.DialogProgressBG()
    progress.create(addon_name)

    for entity_type in entities_by_type.keys():
        # Imposta il messaggio in base al tipo di entità
        msg = {
            'album': 'Processing Albums',
            'song': 'Processing Songs',
            'artist': 'Processing Artists'
        }.get(entity_type, 'Processing Music')

        progress.update(message=msg)
        entities = entities_by_type.get(entity_type)
        total_entities_to_process = len(entities)

        for (step, entity) in enumerate(entities, 1):
            entity_name = entity.get('label')

            # Calcola la percentuale
            percentuale = (step / total_entities_to_process) * 100
            progress.update(percent=int(percentuale), message=entity_name)

            # Filtra e processa tutti i campi che iniziano con 'thumb'
            for entity_field in entity.keys():
                if entity_field.startswith('thumb'):
                    thumbnail = entity.get(entity_field)
                    with xbmcvfs.File(thumbnail):
                        pass

    progress.close()


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


def preload_on_texture_cache():
    db_params = db_scan.get_db_params()
    paths_from_params = db_scan.get_paths_from_params()
    use_webdav = db_params.get('sourcetype') == 'webdav'
    textures = get_textures()
    added_paths = []
    id_albums = []
    if paths_from_params:
        for path in paths_from_params:
            path = db_scan.convert_from_smb_to_davs(path) if use_webdav else path
            if path not in added_paths:
                added_paths.append(path)
    for added_path in added_paths:
        get_id_albums_by_paths(id_albums, added_path)
    entities_by_type = build_entity_map(id_albums, textures)
    cache_medias_textures(entities_by_type)
    execute_addon_with_rpc("script.texture.cache.cleaner")
    monitor = CacheCleanerMonitor()
    if monitor.wait_for_cache_clean():
        xbmc.log('Pulizia della cache delle texture completata', xbmc.LOGINFO)
        monitor.reset()
        builtin_cmd = f'NotifyAll({addon_id}, OnArtsPreloaded)'
        xbmc.executebuiltin(builtin_cmd)


def preload_all_music_cache(api_name, media_type, textures):
    if media_type == 'albums':
        per_page = 1174
    elif media_type == 'songs':
        per_page = 1759
    elif media_type == 'artists':
        per_page = 1155
    start = 0
    end = per_page
    medias_to_process = True
    artists = []
    albums = []
    songs = []
    while medias_to_process:
        medias = get_all_medias(api_name, media_type, start, end)
        medias_to_process = medias and len(medias) > 0
        if media_type == 'artists':
            artists = medias
        elif media_type == 'albums':
            albums = medias
        elif media_type == 'songs':
            songs = medias
        entities_by_type = build_entity_map_by_entities(artists, albums, songs, textures)
        cache_medias_textures(entities_by_type)
        start = end
        end += per_page


def init_music_cache():
    textures = get_textures()
    preload_all_music_cache('GetArtists', 'artists', textures)
    preload_all_music_cache('GetAlbums', 'albums', textures)
    preload_all_music_cache('GetSongs', 'songs', textures)
    execute_addon_with_rpc("script.texture.cache.cleaner")
    monitor = CacheCleanerMonitor()
    if monitor.wait_for_cache_clean():
        xbmc.log('Pulizia della cache delle texture completata', xbmc.LOGINFO)
        monitor.reset()
        builtin_cmd = f'NotifyAll({addon_id}, OnArtsPreloaded)'
        xbmc.executebuiltin(builtin_cmd)


if __name__ == '__main__':
    log(addon_name)
    if db_scan.get_exec_mode() == 'init':
        init_music_cache()
    else:
        preload_on_texture_cache()
