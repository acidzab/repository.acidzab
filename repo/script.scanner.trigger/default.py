import json
import sqlite3

import db_scan
import pymysql
import xbmc
import xbmcaddon
import xbmcgui

addon_name = xbmcaddon.Addon().getAddonInfo('name')
addon_id = xbmcaddon.Addon().getAddonInfo('id')


class ScanMonitor(xbmc.Monitor):
    def __init__(self):
        super(ScanMonitor, self).__init__()
        self.scan_finished = False

    def onScanFinished(self, library):
        if library == 'music':
            self.scan_finished = True

    def wait_for_scan(self):
        """Attende il completamento della scansione"""
        while not self.scan_finished and not self.abortRequested():
            self.waitForAbort(0.5)  # controlla ogni 500ms
        return not self.abortRequested()

    def reset(self):
        """Reset per la prossima scansione"""
        self.scan_finished = False


class AlignMonitor(xbmc.Monitor):
    def __init__(self):
        super(AlignMonitor, self).__init__()
        self.align_finished = False

    def onNotification(self, sender, method, data):
        texture_refresh_done = sender == 'script.texture.refresh' and method == 'Other.OnTextureRefreshed'
        if texture_refresh_done:
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


def execute_addon_with_builtin(addon_id, params):
    builtin_cmd = f'RunAddon({addon_id},{params})'
    xbmc.executebuiltin(builtin_cmd, True)


def scan_folders(paths_to_scan):
    monitor = ScanMonitor()
    for path_to_scan in paths_to_scan:
        monitor.reset()
        scan_payload = {"jsonrpc": "2.0", "method": "AudioLibrary.Scan", "id": "1",
                        "params": {"directory": path_to_scan}}
        xbmc.executeJSONRPC(json.dumps(scan_payload, ensure_ascii=False))
        if monitor.wait_for_scan():
            xbmc.log(f"Scansione completata per: {path_to_scan}", xbmc.LOGINFO)


def get_song_by_file(id_albums, call_central, db_params):
    songs_by_id_album = get_songs_by_albums(id_albums, call_central, db_params)
    song_by_file = {}
    if songs_by_id_album:
        for id_album in songs_by_id_album:
            for song in songs_by_id_album.get(id_album):
                song_by_file[song.get('file')] = song
    return song_by_file


def get_directory(path, call_central, db_params):
    get_directory_payload = {
        "jsonrpc": "2.0",
        "method": "Files.GetDirectory",
        "id": "1",
        "params": {
            "directory": path,
            "media": "music",
            "properties": [
                "albumid",
                "artistid",
                "albumartistid",
                "musicbrainzalbumid"
            ],
            "sort": {
                "method": "file"
            }
        }
    }
    if call_central:
        get_directory_response = db_scan.execute_from_central_kodi_webserver(db_params, get_directory_payload).get(
            'result')
    else:
        get_directory_response = json.loads(
            xbmc.executeJSONRPC(json.dumps(get_directory_payload, ensure_ascii=False))).get('result')
    return get_directory_response


def get_media_paths_to_process(media_details_by_filename, path, call_central, db_params):
    directory_response = get_directory(path, call_central, db_params)
    if directory_response:
        for file_response in directory_response.get('files'):
            if file_response.get('filetype') == 'directory':
                if file_response.get('type') == 'unknown':
                    get_media_paths_to_process(media_details_by_filename, file_response.get('file'), call_central,
                                               db_params)
                else:
                    media_details_by_filename[file_response.get('file')] = file_response
            elif file_response.get('type') == 'song':
                media_details_by_filename[file_response.get('file')] = file_response

    return media_details_by_filename


def get_all_medias(media_type, db_params, music_db_name):
    """
    Aggrega dati di album o brani tra database MariaDB centrale e SQLite locale.
    Per gli album usa MusicBrainz ID, per i brani usa tupla (titolo, album_mbid).
    """
    if media_type not in ['album', 'song']:
        raise ValueError("media_type deve essere 'album' o 'song'")

    if media_type == 'album':
        return _get_albums_aggregated(db_params, music_db_name)
    else:
        return _get_songs_aggregated(db_params, music_db_name)


def _get_albums_aggregated(db_params, music_db_name):
    """Aggrega album usando MusicBrainz Album ID"""
    album_query = '''
                  SELECT album.idAlbum,
                         album.strMusicBrainzAlbumID as mbid
                  FROM album
                  WHERE album.strMusicBrainzAlbumID IS NOT NULL'''

    # Recupera dati centrali
    central_albums = {}
    central_results = []
    host = db_params.get('host')
    username = db_params.get('user')
    password = db_params.get('pass')
    central_db = pymysql.connect(host=host, user=username, password=password, database=music_db_name, port=3306,
                                 cursorclass=pymysql.cursors.DictCursor, connect_timeout=18000)
    with central_db:
        with central_db.cursor() as central_cursor:
            central_cursor.execute(album_query)
            log(central_cursor.mogrify(album_query))
            central_results.extend(central_cursor.fetchall())
    if central_results:
        central_albums = {row.get('mbid'): row.get('idAlbum') for row in central_results}

    # Recupera dati locali
    local_albums = {}
    local_results = []
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.row_factory = sqlite3.Row
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    music_db_cursor.execute(album_query)
    local_results.extend(music_db_cursor.fetchall())
    music_db_cursor.close()
    music_db.close()
    if local_results:
        local_albums = {row['mbid']: row['idAlbum'] for row in local_results}

    # Aggrega risultati
    aggregated_albums = []
    for mbid in central_albums.keys():
        album_data = {
            'id': central_albums[mbid],
            'localid': local_albums.get(mbid),
            'mbid': mbid
        }
        aggregated_albums.append(album_data)

    return aggregated_albums


def _get_songs_aggregated(db_params, music_db_name):
    """
    Aggrega brani usando tupla (titolo normalizzato, album MBID) come chiave univoca
    """
    song_query = '''
                 SELECT song.idSong,
                        song.strTitle,
                        album.strMusicBrainzAlbumID as album_mbid
                 FROM song
                          JOIN album ON album.idAlbum = song.idAlbum
                 WHERE album.strMusicBrainzAlbumID IS NOT NULL'''

    # Recupera dati centrali
    central_songs = {}
    central_results = []
    host = db_params.get('host')
    username = db_params.get('user')
    password = db_params.get('pass')
    central_db = pymysql.connect(host=host, user=username, password=password, database=music_db_name, port=3306,
                                 cursorclass=pymysql.cursors.DictCursor, connect_timeout=18000)
    with central_db:
        with central_db.cursor() as central_cursor:
            central_cursor.execute(song_query)
            log(central_cursor.mogrify(song_query))
            central_results.extend(central_cursor.fetchall())
    if central_results:
        for row in central_results:
            title = row.get('strTitle')
            album_mbid = row.get('album_mbid')
            if title and album_mbid:
                # Crea chiave univoca: titolo normalizzato + album MBID
                key = _create_song_key(title, album_mbid)
                central_songs[key] = {
                    'id': row.get('idSong'),
                    'title': title,
                    'album_mbid': album_mbid
                }

    # Recupera dati locali
    local_songs = {}
    local_results = []
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.row_factory = sqlite3.Row
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    music_db_cursor.execute(song_query)
    local_results.extend(music_db_cursor.fetchall())
    music_db_cursor.close()
    music_db.close()
    if local_results:
        for row in local_results:
            title = row['strTitle']
            album_mbid = row['album_mbid']

            if title and album_mbid:
                # Crea chiave univoca: titolo normalizzato + album MBID
                key = _create_song_key(title, album_mbid)
                local_songs[key] = {
                    'id': row['idSong'],
                    'title': title,
                    'album_mbid': album_mbid
                }

    # Aggrega risultati
    aggregated_songs = []
    for key, central_song in central_songs.items():
        local_song = local_songs.get(key)

        song_data = {
            'id': central_song['id'],
            'localid': local_song['id'] if local_song else None,
            'title': central_song['title'],
            'album_mbid': central_song['album_mbid']
        }
        aggregated_songs.append(song_data)

    return aggregated_songs


def _create_song_key(title, album_mbid):
    """
    Crea una chiave univoca per il matching dei brani.
    Normalizza il titolo per gestire piccole differenze di formattazione.
    """
    # Normalizzazione del titolo:
    # - Lowercase
    # - Strip spazi iniziali/finali
    # - Riduce spazi multipli a singoli
    # - Rimuove caratteri problematici comuni
    normalized_title = title.lower().strip()
    normalized_title = ' '.join(normalized_title.split())  # Riduce spazi multipli

    # Opzionalmente, rimuovi caratteri che potrebbero variare tra database
    # normalized_title = normalized_title.replace("'", "").replace('"', "")

    return f"{normalized_title}|{album_mbid}"


def get_media_details_from_directory(scanned_paths, scanned_paths_local, db_params):
    central_response_by_filename = {}
    local_response_by_filename = {}
    media_details_by_id = {}
    use_webdav = db_params.get('sourcetype') == 'webdav'
    for scanned_path in scanned_paths:
        get_media_paths_to_process(central_response_by_filename, scanned_path, True, db_params)
    for local_path in scanned_paths_local:
        get_media_paths_to_process(local_response_by_filename, local_path, False, db_params)

    central_ids = []
    local_ids = []
    for file in central_response_by_filename.keys():
        local_key = db_scan.convert_from_smb_to_davs(file) if use_webdav else file
        central_file = central_response_by_filename.get(file)
        local_file = local_response_by_filename.get(local_key)
        if central_file:
            central_ids.append(central_file.get('albumid'))
        if local_file:
            local_ids.append(local_file.get('albumid'))

    central_songs_by_file = get_song_by_file(central_ids, True, db_params)
    local_songs_by_file = get_song_by_file(local_ids, False, db_params)

    for file in central_response_by_filename.keys():
        local_key = db_scan.convert_from_smb_to_davs(file) if use_webdav else file
        central_file = central_response_by_filename.get(file)
        local_file = local_response_by_filename.get(local_key)
        if central_file and local_file:
            if central_file.get('type') == 'album':
                songs = []
                central_file_album_id = central_file.get('albumid')
                if central_songs_by_file and local_songs_by_file:
                    for central_song_path in central_songs_by_file.keys():
                        central_song = central_songs_by_file.get(central_song_path)
                        if central_song.get('albumid') == central_file_album_id:
                            local_song_path = db_scan.convert_from_smb_to_davs(
                                central_song_path) if use_webdav else central_song_path
                            local_song = local_songs_by_file.get(local_song_path)
                            if local_song:
                                central_song['localsongid'] = local_song.get('songid')
                                central_song['localartistid'] = local_song.get('artistid')
                            if central_song not in songs:
                                songs.append(central_song)
                central_file['songs'] = songs
            central_file['localalbumid'] = local_file.get('albumid')
            # per gli artisti mi servono gli id di entrambe le istanze
            central_file['localartistid'] = local_file.get('artistid')
            central_file['localalbumartistid'] = local_file.get('albumartistid')
            central_song = central_songs_by_file.get(file)
            # workaround: forzo l'mbid album dalla singola traccia se non lo ricevo dalla GetDirectory
            if not central_file.get('musicbrainzalbumid') and central_song and central_song.get('musicbrainzalbumid'):
                central_file['musicbrainzalbumid'] = central_song.get('musicbrainzalbumid')
            media_details_by_id[local_file.get('id')] = central_file

    return media_details_by_id


def get_songs_by_albums(id_albums, call_central, db_params):
    json_payloads = []
    songs_by_id_album = {}
    for (id_rpc, id_album) in enumerate(id_albums, 1):
        json_payload = {
            "jsonrpc": "2.0",
            "method": "AudioLibrary.GetSongs",
            "id": id_rpc,
            "params": {
                "properties": [
                    "albumid",
                    "artistid",
                    "file",
                    "musicbrainzalbumid"
                ],
                "filter": {
                    "albumid": id_album
                }
            }
        }
        json_payloads.append(json_payload)
    splitted_requests = db_scan.split_json(json_payloads)
    for splitted_request in splitted_requests:
        if call_central:
            response_get_songs = db_scan.execute_from_central_kodi_webserver(db_params, splitted_request)
        else:
            response_get_songs = json.loads(xbmc.executeJSONRPC(json.dumps(splitted_request, ensure_ascii=False)))

        for json_response in response_get_songs:
            result = json_response.get('result')
            album_songs = result.get('songs')
            for song in album_songs:
                album_id = song.get('albumid')
                songs_by_id_album[album_id] = album_songs
                break
    return songs_by_id_album


def get_artworks_by_key(id_albums, media_type, db_params, call_central, music_db_name):
    artworks_by_key = {}
    art_results = []
    album_query = '''
                  SELECT album.idAlbum,
                         album.strMusicBrainzAlbumID,
                         art.type,
                         art.url
                  FROM album
                           JOIN art ON art.media_id = album.idAlbum
                      AND art.media_type = \'album\''''
    song_query = '''
                 SELECT song.idSong,
                        song.strTitle,
                        album.strMusicBrainzAlbumID,
                        art.type,
                        art.url
                 FROM song
                          JOIN album ON song.idAlbum = album.idAlbum
                          JOIN art ON art.media_id = song.idSong
                     AND art.media_type = \'song\''''
    if media_type == 'album':
        query = album_query
    elif media_type == 'song':
        query = song_query
    if id_albums:
        album_in_condition = ''' WHERE album.idAlbum IN (%s)'''
        query += album_in_condition

    if call_central:
        host = db_params.get('host')
        username = db_params.get('user')
        password = db_params.get('pass')
        central_db = pymysql.connect(host=host, user=username, password=password, database=music_db_name, port=3306,
                                     cursorclass=pymysql.cursors.DictCursor, connect_timeout=18000)
        with central_db:
            with central_db.cursor() as central_cursor:
                if id_albums:
                    chunks = [id_albums[i:i + 1000] for i in range(0, len(id_albums), 1000)]
                    for chunk in chunks:
                        placeholders = ','.join(['%s'] * len(chunk))
                        central_cursor.execute(query % placeholders, chunk)
                        log(central_cursor.mogrify(query % placeholders, chunk))
                        art_results.extend(central_cursor.fetchall())
                else:
                    central_cursor.execute(query)
                    log(central_cursor.mogrify(query))
                    art_results.extend(central_cursor.fetchall())
        if art_results:
            for art_result in art_results:
                key = (art_result.get('idAlbum'),
                       art_result.get('strMusicBrainzAlbumID')) if media_type == 'album' else (
                    art_result.get('idSong'), art_result.get('strMusicBrainzAlbumID'), art_result.get('strTitle'))
                art_info = artworks_by_key.get(key)
                if not art_info:
                    art_info = {
                        art_result.get('type'): art_result.get('url')
                    }
                else:
                    art_info[art_result.get('type')] = art_result.get('url')
                artworks_by_key[key] = art_info
    else:
        music_db_path = db_scan.get_music_db_path()
        music_db = sqlite3.connect(music_db_path)
        music_db.row_factory = sqlite3.Row
        music_db.set_trace_callback(log)
        music_db_cursor = music_db.cursor()
        if id_albums:
            chunks = [id_albums[i:i + 999] for i in range(0, len(id_albums), 999)]
            for chunk in chunks:
                placeholders = ','.join(['?'] * len(chunk))
                music_db_cursor.execute(query % placeholders, chunk)
                art_results.extend(music_db_cursor.fetchall())
        else:
            music_db_cursor.execute(query)
            art_results.extend(music_db_cursor.fetchall())
        music_db_cursor.close()
        music_db.close()
        if art_results:
            for art_result in art_results:
                key = (art_result['idAlbum'], art_result['strMusicBrainzAlbumID']) if media_type == 'album' else (
                    art_result['idSong'], art_result['strMusicBrainzAlbumID'], art_result['strTitle'])
                art_info = artworks_by_key.get(key)
                if not art_info:
                    art_info = {
                        art_result['type']: art_result['url']
                    }
                else:
                    art_info[art_result['type']] = art_result['url']
                artworks_by_key[key] = art_info
    return artworks_by_key


def get_artists_data(id_artists_set, db_params, call_central, music_db_name):
    id_artists = []
    artists_data = []
    discography_by_artist = {}
    artist_results = []
    artist_discography_results = []
    base_artist_query = '''
                        SELECT DISTINCT idArtist,
                                        strArtist,
                                        strMusicBrainzArtistID,
                                        strType,
                                        strGender,
                                        strDisambiguation,
                                        strBorn,
                                        strFormed,
                                        strGenres,
                                        strMoods,
                                        strStyles,
                                        strInstruments,
                                        strBiography,
                                        strDied,
                                        strDisbanded,
                                        strYearsActive,
                                        strImage,
                                        art.url
                        FROM artist
                                 LEFT JOIN art ON art.media_id = artist.idArtist AND art.media_type = \'artist\''''
    base_artist_discography_query = f'''SELECT * FROM discography'''
    artist_query = base_artist_query
    artist_discography_query = base_artist_discography_query
    if id_artists_set:
        id_artists = list(id_artists_set)
        artist_in_condition = ''' WHERE idArtist IN (%s)'''
        artist_query += artist_in_condition
        artist_discography_query += artist_in_condition

    if call_central:
        host = db_params.get('host')
        username = db_params.get('user')
        password = db_params.get('pass')
        central_db = pymysql.connect(host=host, user=username, password=password, database=music_db_name, port=3306,
                                     cursorclass=pymysql.cursors.DictCursor, connect_timeout=18000)
        with central_db:
            with central_db.cursor() as central_cursor:
                if id_artists:
                    chunks = [id_artists[i:i + 1000] for i in range(0, len(id_artists), 1000)]
                    for chunk in chunks:
                        placeholders = ','.join(['%s'] * len(chunk))
                        central_cursor.execute(artist_query % placeholders, chunk)
                        log(central_cursor.mogrify(artist_query % placeholders, chunk))
                        artist_results.extend(central_cursor.fetchall())
                        central_cursor.execute(artist_discography_query % placeholders, chunk)
                        log(central_cursor.mogrify(artist_discography_query % placeholders, chunk))
                        artist_discography_results.extend(central_cursor.fetchall())
                else:
                    central_cursor.execute(artist_query)
                    log(central_cursor.mogrify(artist_query))
                    artist_results.extend(central_cursor.fetchall())
                    central_cursor.execute(artist_discography_query)
                    log(central_cursor.mogrify(artist_discography_query))
                    artist_discography_results.extend(central_cursor.fetchall())
        if artist_discography_results:
            for artist_discography_result in artist_discography_results:
                discography = discography_by_artist.get(artist_discography_result.get('idArtist'))
                if not discography:
                    discography = []
                if artist_discography_result.get('strAlbum') and artist_discography_result.get('strYear'):
                    discography_info = {
                        'album': artist_discography_result.get('strAlbum'),
                        'year': artist_discography_result.get('strYear'),
                        'mbid': artist_discography_result.get('strReleaseGroupMBID')
                    }
                    if discography_info not in discography:
                        discography.append(discography_info)
                discography_by_artist[artist_discography_result.get('idArtist')] = discography
        if artist_results:
            for artist in artist_results:
                artist_info = {
                    'id': artist.get('idArtist'),
                    'name': artist.get('strArtist'),
                    'mbid': artist.get('strMusicBrainzArtistID'),
                    'disambiguation': artist.get('strDisambiguation'),
                    'genres': artist.get('strGenres'),
                    'biography': artist.get('strBiography'),
                    'type': artist.get('strType'),
                    'gender': artist.get('strGender'),
                    'born': artist.get('strBorn'),
                    'years_active': artist.get('strYearsActive'),
                    'moods': artist.get('strMoods'),
                    'styles': artist.get('strStyles'),
                    'instruments': artist.get('strInstruments'),
                    'formed': artist.get('strFormed'),
                    'died': artist.get('strDied'),
                    'disbanded': artist.get('strDisbanded'),
                    'images': artist.get('strImage'),
                    'art_url': artist.get('url')
                }
                artist_discography = discography_by_artist.get(artist.get('idArtist'))
                if artist_discography:
                    artist_info['discography'] = artist_discography
                if artist_info not in artists_data:
                    artists_data.append(artist_info)
    else:
        music_db_path = db_scan.get_music_db_path()
        music_db = sqlite3.connect(music_db_path)
        music_db.row_factory = sqlite3.Row
        music_db.set_trace_callback(log)
        music_db_cursor = music_db.cursor()
        if id_artists:
            chunks = [id_artists[i:i + 999] for i in range(0, len(id_artists), 999)]
            for chunk in chunks:
                placeholders = ','.join(['?'] * len(chunk))
                music_db_cursor.execute(artist_query % placeholders, chunk)
                artist_results.extend(music_db_cursor.fetchall())
                music_db_cursor.execute(artist_discography_query % placeholders, chunk)
                artist_discography_results.extend(music_db_cursor.fetchall())
        else:
            music_db_cursor.execute(artist_query)
            artist_results.extend(music_db_cursor.fetchall())
            music_db_cursor.execute(artist_discography_query)
            artist_discography_results.extend(music_db_cursor.fetchall())
        music_db_cursor.close()
        music_db.close()
        if artist_discography_results:
            for artist_discography_result in artist_discography_results:
                discography = discography_by_artist.get(artist_discography_result['idArtist'])
                if not discography:
                    discography = []
                if artist_discography_result['strAlbum'] and artist_discography_result['strYear']:
                    discography_info = {
                        'album': artist_discography_result['strAlbum'],
                        'year': artist_discography_result['strYear'],
                        'mbid': artist_discography_result['strReleaseGroupMBID']
                    }
                    if discography_info not in discography:
                        discography.append(discography_info)
                discography_by_artist[artist_discography_result['idArtist']] = discography
        if artist_results:
            for artist_result in artist_results:
                artist_info = {
                    'id': artist_result['idArtist'],
                    'name': artist_result['strArtist'],
                    'mbid': artist_result['strMusicBrainzArtistID'],
                    'disambiguation': artist_result['strDisambiguation'],
                    'genres': artist_result['strGenres'],
                    'biography': artist_result['strBiography'],
                    'type': artist_result['strType'],
                    'gender': artist_result['strGender'],
                    'born': artist_result['strBorn'],
                    'years_active': artist_result['strYearsActive'],
                    'moods': artist_result['strMoods'],
                    'styles': artist_result['strStyles'],
                    'instruments': artist_result['strInstruments'],
                    'formed': artist_result['strFormed'],
                    'died': artist_result['strDied'],
                    'disbanded': artist_result['strDisbanded'],
                    'images': artist_result['strImage'],
                    'art_url': artist_result['url']
                }
                artist_discography = discography_by_artist.get(artist_result['idArtist'])
                if artist_discography:
                    artist_info['discography'] = artist_discography
                if artist_info not in artists_data:
                    artists_data.append(artist_info)
    return artists_data


def update_arts(arts_to_insert, arts_to_update, arts_to_remove):
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    if arts_to_insert:
        query = '''INSERT INTO art (media_id, media_type, type, url)
                   VALUES (?, ?, ?, ?)'''
        music_db_cursor.executemany(query, arts_to_insert)
        music_db.commit()
    if arts_to_remove:
        query = '''
                DELETE
                FROM art
                WHERE media_id = ?
                  AND media_type = ?
                  AND type = ?'''
        music_db_cursor.executemany(query, arts_to_remove)
        music_db.commit()
    if arts_to_update:
        query = '''
                UPDATE art
                SET url = ?
                WHERE media_id = ?
                  AND media_type = ?
                  AND type = ?'''
        music_db_cursor.executemany(query, arts_to_update)
        music_db.commit()
    music_db_cursor.close()
    music_db.close()


def update_artist_records(central_artists, local_artists, artists_to_update):
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    update_query = '''
                   UPDATE artist
                   SET strArtist=?,
                       strDisambiguation=?,
                       strGenres=?,
                       strBiography=?,
                       strType=?,
                       strGender=?,
                       strBorn=?,
                       strFormed=?,
                       strMoods=?,
                       strStyles=?,
                       strInstruments=?,
                       strDied=?,
                       strDisbanded=?,
                       strYearsActive=?,
                       strImage=?,
                       lastScraped=CURRENT_TIMESTAMP
                   WHERE idArtist = ?'''
    artists_value_to_set = []
    for central_mbid in artists_to_update:
        artist_to_set = central_artists.get(central_mbid)
        local_artist = local_artists.get(central_mbid)
        if artist_to_set and local_artist:
            artist_values_to_set = (
                artist_to_set.get('name'),
                artist_to_set.get('disambiguation'),
                artist_to_set.get('genres'),
                artist_to_set.get('biography'),
                artist_to_set.get('type'),
                artist_to_set.get('gender'),
                artist_to_set.get('born'),
                artist_to_set.get('formed'),
                artist_to_set.get('moods'),
                artist_to_set.get('styles'),
                artist_to_set.get('instruments'),
                artist_to_set.get('died'),
                artist_to_set.get('disbanded'),
                artist_to_set.get('years_active'),
                artist_to_set.get('images'),
                local_artist.get('id')
            )
            if artist_values_to_set not in artists_value_to_set:
                artists_value_to_set.append(artist_values_to_set)
    if artists_value_to_set:
        music_db_cursor.executemany(update_query, artists_value_to_set)
        music_db.commit()
    releases_to_set = []
    discography_delete_query = 'DELETE FROM discography WHERE idArtist = ?'
    discography_insert_query = '''INSERT INTO discography (idArtist, strAlbum, strYear, strReleaseGroupMBID)
                                  VALUES (?, ?, ?, ?)'''
    artists_to_reset = []
    for central_mbid in artists_to_update:
        local_artist = local_artists.get(central_mbid)
        central_artist = central_artists.get(central_mbid)
        if central_artist.get('discography'):
            artists_to_reset.append((local_artist.get('id'),))
            for release in central_artist.get('discography'):
                release_to_set = (local_artist.get('id'), release.get('album'), release.get('year'),
                                  release.get('mbid'))
                if release_to_set not in releases_to_set:
                    releases_to_set.append(release_to_set)
        elif local_artist.get('discography') and not central_artist.get('discography'):
            artists_to_reset.append((local_artist.get('id'),))
    if releases_to_set or artists_to_reset:
        music_db_cursor.executemany(discography_delete_query, artists_to_reset)
        music_db.commit()
    if releases_to_set:
        music_db_cursor.executemany(discography_insert_query, releases_to_set)
        music_db.commit()
    music_db_cursor.close()
    music_db.close()


# una bella compattata al db non guasta dopo tutto questo smarmellaio
def compact_db():
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    query = '''VACUUM'''
    music_db_cursor.execute(query)
    music_db_cursor.close()
    music_db.close()


def clean_paths():
    query = '''DELETE
               FROM path
               WHERE idPath IN
                     (SELECT p.idPath
                      FROM path p
                      WHERE p.idPath NOT IN
                            (SELECT idPath
                             FROM song)
                        AND NOT EXISTS
                          (SELECT 1
                           FROM path child
                           WHERE child.strPath LIKE p.strPath || '%'
                             AND child.strPath != p.strPath)
                      ORDER BY p.strPath)'''
    music_db_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(music_db_path)
    music_db.row_factory = sqlite3.Row
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    music_db_cursor.execute(query)
    music_db.commit()
    music_db_cursor.close()
    music_db.close()


def force_path_rescan(paths_to_scan):
    music_db_path = db_scan.get_music_db_path()
    query = '''
            SELECT path.idPath
            FROM PATH
                     LEFT JOIN song ON song.idPath = path.idPath
            WHERE song.idAlbum IS NULL
              AND path.strPath IN (%s)
            '''
    query_results = []
    music_db = sqlite3.connect(music_db_path)
    music_db.row_factory = sqlite3.Row
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    chunks = [paths_to_scan[i:i + 999] for i in range(0, len(paths_to_scan), 999)]
    for chunk in chunks:
        placeholders = ','.join(['?'] * len(chunk))
        music_db_cursor.execute(query % placeholders, chunk)
        query_results.extend(music_db_cursor.fetchall())
    music_db_cursor.close()
    music_db.close()
    ids_to_force = [result['idPath'] for result in query_results]
    if ids_to_force:
        paths_to_force = []
        for id_to_force in ids_to_force:
            paths_to_force.append((id_to_force,))
        update_query = 'UPDATE path SET strHash=\'\' WHERE idPath=?'
        music_db = sqlite3.connect(music_db_path)
        music_db.row_factory = sqlite3.Row
        music_db.set_trace_callback(log)
        music_db_cursor = music_db.cursor()
        music_db_cursor.executemany(update_query, paths_to_force)
        music_db.commit()
        music_db_cursor.close()
        music_db.close()


def replace_local_art_with_artworker(id_album, db_params, central_art):
    artwork = db_scan.get_manual_arts_from_artworker(db_params, id_album)
    if artwork and central_art and artwork.get('media_id') == id_album and artwork.get('manual_artwork'):
        for art_field in central_art:
            art_url = central_art.get(art_field)
            if art_url.startswith('smb'):
                artworker_id = artwork.get('id')
                artworker_media_id = artwork.get('media_id')
                artworker_media_type = artwork.get('media_type')
                artworker_host = db_params.get('artworkerbasehost')
                artworker_to_set = f'{artworker_host}/media/{artworker_media_id}/{artworker_media_type}/arts/{artworker_id}'
                central_art[art_field] = artworker_to_set


def align_media_to_central_db(paths, local_paths, exec_mode, db_params):
    progress = xbmcgui.DialogProgressBG()
    media_by_id = get_media_details_from_directory(paths, local_paths, db_params)
    music_db_name = db_scan.get_latest_kodi_dbs().get('MyMusic')
    albums_id_central = []
    albums_id_local = []
    songs_id_central = []
    songs_id_local = []
    artists_id_central = set()
    artists_id_local = set()
    arts_to_insert = set()
    arts_to_update = set()
    arts_to_remove = set()
    # qui raccolgo gli id album che hanno come artwork un file e che devo recepire tramite artworker
    id_albums_with_local_art = []

    for media_id in media_by_id:
        # id per album e brani
        media = media_by_id.get(media_id)
        if media.get('albumid') and media.get('albumid') not in albums_id_central:
            albums_id_central.append(media.get('albumid'))
        if media.get('localalbumid') and media.get('localalbumid') not in albums_id_local:
            albums_id_local.append(media.get('localalbumid'))
        if media.get('type') == 'album' and media.get('songs'):
            for song in media.get('songs'):
                if song.get('songid') and song.get('songid') not in songs_id_central:
                    songs_id_central.append(song.get('songid'))
                if song.get('localsongid') and song.get('localsongid') not in songs_id_local:
                    songs_id_local.append(song.get('localsongid'))
                if song.get('artistid'):
                    artists_id_central.update(song.get('artistid'))
                if song.get('localartistid'):
                    artists_id_local.update(song.get('localartistid'))
        else:
            if media.get('type') == 'song' and media.get('id') not in songs_id_central:
                songs_id_central.append(media.get('id'))
            if media.get('type') == 'song' and media_id not in songs_id_local:
                songs_id_local.append(media_id)
        # id per gli artisti
        artists_id_central.update(media.get('artistid'))
        if media.get('albumartistid'):
            artists_id_local.update(media.get('albumartistid'))
        artists_id_local.update(media.get('localartistid'))
        if media.get('localalbumartistid'):
            artists_id_local.update(media.get('localalbumartistid'))

    # Allineo i dati degli artisti col db centrale
    progress.create(addon_name, message='Allineo i dati artista')
    central_artists_data = get_artists_data(artists_id_central, db_params, True, music_db_name)
    local_artists_data = get_artists_data(artists_id_local, db_params, False, music_db_name)
    central_artists_by_mbid = {artist.get('mbid'): artist for artist in central_artists_data}
    local_artists_by_mbid = {artist.get('mbid'): artist for artist in local_artists_data}
    artists_to_update = []
    for artist_mbid in central_artists_by_mbid.keys():
        central_artist = central_artists_by_mbid.get(artist_mbid)
        local_artist = local_artists_by_mbid.get(artist_mbid)
        if central_artist:
            original_central_id_artist = central_artist['id']
        if central_artist and local_artist:
            central_artist['id'] = local_artist['id']
            if local_artist != central_artist:
                artists_to_update.append(artist_mbid)
            central_artist['id'] = original_central_id_artist
    update_artist_records(central_artists_by_mbid, local_artists_by_mbid, artists_to_update)

    progress.update(message='Allineo gli artwork')
    central_album_arts = get_artworks_by_key(albums_id_central, 'album', db_params, True, music_db_name)
    local_album_arts = get_artworks_by_key(albums_id_local, 'album', db_params, False, music_db_name)
    central_song_arts = get_artworks_by_key(albums_id_central, 'song', db_params, True, music_db_name)
    local_song_arts = get_artworks_by_key(albums_id_local, 'song', db_params, False, music_db_name)

    if media_by_id:
        for media_id in media_by_id:
            media = media_by_id.get(media_id)
            central_album_art = central_album_arts.get((media.get('albumid'), media.get('musicbrainzalbumid')))
            if central_album_art:
                for art_field in central_album_art.keys():
                    art_url = central_album_art.get(art_field)
                    if art_url.startswith('smb'):
                        id_albums_with_local_art.append(media.get('albumid'))

        for media_id in media_by_id:
            media = media_by_id.get(media_id)
            if media and media.get('localalbumid'):
                central_album_art = central_album_arts.get((media.get('albumid'), media.get('musicbrainzalbumid')))
                local_album_art = local_album_arts.get((media.get('localalbumid'), media.get('musicbrainzalbumid')))
                if media.get('albumid') in id_albums_with_local_art:
                    replace_local_art_with_artworker(media.get('albumid'), db_params, central_album_art)
                _prepare_art_tuples_optimized(arts_to_insert, arts_to_remove, arts_to_update, central_album_art,
                                              local_album_art,
                                              media.get('localalbumid'), 'album')
        for media_id in media_by_id:
            media_details = media_by_id.get(media_id)
            if media_details:
                id_song = media_id
                if media_details.get('type') == 'album' and media_details.get('songs'):
                    for song in media_details.get('songs'):
                        id_song = song.get('localsongid')
                        central_key = (song.get('songid'), song.get('musicbrainzalbumid'), song.get('label'))
                        local_key = (id_song, song.get('musicbrainzalbumid'), song.get('label'))
                        central_song_art = central_song_arts.get(central_key)
                        local_song_art = local_song_arts.get(local_key)
                        _prepare_art_tuples_optimized(arts_to_insert, arts_to_remove, arts_to_update, central_song_art,
                                                      local_song_art,
                                                      id_song, 'song')
                else:
                    central_key = (media_details.get('id'), media_details.get('musicbrainzalbumid'),
                                   media_details.get('label'))
                    central_song_art = central_song_arts.get(central_key)
                    local_key = (id_song, media_details.get('musicbrainzalbumid'), media_details.get('label'))
                    local_song_art = local_song_arts.get(local_key)
                    _prepare_art_tuples_optimized(arts_to_insert, arts_to_remove, arts_to_update, central_song_art,
                                                  local_song_art,
                                                  id_song, 'song')

                for central_mbid in central_artists_by_mbid.keys():
                    central_artist_info = central_artists_by_mbid.get(central_mbid)
                    local_artist_info = local_artists_by_mbid.get(central_mbid)
                    if central_artist_info and local_artist_info:
                        central_artist_art = {'thumb': central_artist_info.get('art_url')} if central_artist_info.get(
                            'art_url') else None
                        local_artist_art = {'thumb': local_artist_info.get('art_url')} if local_artist_info.get(
                            'art_url') else None
                        central_artist_info['id'] = local_artist_info.get('id')
                        _prepare_art_tuples_optimized(arts_to_insert, arts_to_remove, arts_to_update,
                                                      central_artist_art,
                                                      local_artist_art,
                                                      local_artist_info.get('id'), 'artist')
    else:
        processed_arts = process_media_art_with_batching(db_params, music_db_name, central_album_arts, local_album_arts,
                                                         central_song_arts, local_song_arts, central_artists_by_mbid,
                                                         local_artists_by_mbid)
        arts_to_insert = processed_arts.get('arts_to_insert')
        arts_to_remove = processed_arts.get('arts_to_remove')
        arts_to_update = processed_arts.get('arts_to_update')

    update_arts(list(arts_to_insert), list(arts_to_update), list(arts_to_remove))
    progress.close()
    compact_db()

    query_string = ';'.join([f"path={path}" for path in paths if path])
    params = db_scan.encode_string(f'?{query_string};mode={exec_mode}', safe_chars='()!')
    execute_addon_with_builtin('script.texture.refresh', params)


def _prepare_art_tuples_optimized(arts_to_insert, arts_to_remove, arts_to_update,
                                  central_art, local_art, media_id, media_type):
    """
    Versione ottimizzata di prepare_art_tuples che usa set invece di liste
    e riduce i controlli di duplicati da O(n) a O(1)
    """

    # Gestione artwork da inserire
    if central_art:
        for art_type, art_url in central_art.items():
            if not local_art or not local_art.get(art_type):
                arts_to_insert.add((media_id, media_type, art_type, art_url))

    # Gestione artwork da rimuovere/aggiornare
    if local_art:
        for art_type, local_url in local_art.items():
            # Rimuovi se non esiste nel centrale
            if not central_art or not central_art.get(art_type):
                arts_to_remove.add((media_id, media_type, art_type))
            # Aggiorna se diverso
            elif central_art.get(art_type) != local_url:
                central_url = central_art.get(art_type)
                arts_to_update.add((central_url, media_id, media_type, art_type))


def process_media_art_with_batching(db_params, music_db_name, central_album_arts, local_album_arts,
                                    central_song_arts, local_song_arts, central_artists_by_mbid, local_artists_by_mbid,
                                    batch_size=1759):
    """
    Versione con batching per dataset molto grandi (50k+ elementi)
    Processa i dati in batch per ridurre memory pressure
    """

    log(f"Inizio elaborazione artwork con batching (batch_size={batch_size})...")

    # Risultati accumulati
    all_arts_to_insert = set()
    all_arts_to_remove = set()
    all_arts_to_update = set()
    id_albums_with_local_art = set()

    # Ottieni dati
    albums = get_all_medias('album', db_params, music_db_name)
    songs = get_all_medias('song', db_params, music_db_name)

    # Elabora album in batch
    for i in range(0, len(albums), batch_size):
        batch = albums[i:i + batch_size]
        log(f"Elaborazione batch album {i // batch_size + 1}/{(len(albums) - 1) // batch_size + 1}")

        batch_results = _process_album_batch(
            batch, central_album_arts, local_album_arts,
            id_albums_with_local_art, db_params
        )

        log(batch_results)

        all_arts_to_insert.update(batch_results['insert'])
        all_arts_to_remove.update(batch_results['remove'])
        all_arts_to_update.update(batch_results['update'])

    # Elabora brani in batch
    for i in range(0, len(songs), batch_size):
        batch = songs[i:i + batch_size]
        log(f"Elaborazione batch brani {i // batch_size + 1}/{(len(songs) - 1) // batch_size + 1}")

        batch_results = _process_song_batch(
            batch, central_song_arts, local_song_arts
        )

        log(batch_results)

        all_arts_to_insert.update(batch_results['insert'])
        all_arts_to_remove.update(batch_results['remove'])
        all_arts_to_update.update(batch_results['update'])

    # Elabora artisti (generalmente pochi, non serve batching)
    artist_results = _process_artists(central_artists_by_mbid, local_artists_by_mbid)
    all_arts_to_insert.update(artist_results['insert'])
    all_arts_to_remove.update(artist_results['remove'])
    all_arts_to_update.update(artist_results['update'])

    result = {
        'arts_to_insert': list(all_arts_to_insert),
        'arts_to_remove': list(all_arts_to_remove),
        'arts_to_update': list(all_arts_to_update)
    }

    return result


def _process_album_batch(album_batch, central_album_arts, local_album_arts, id_albums_with_local_art, db_params):
    """Elabora un batch di album"""
    arts_to_insert = set()
    arts_to_remove = set()
    arts_to_update = set()

    for album in album_batch:
        album_id = album.get('id')
        album_mbid = album.get('mbid')
        local_id = album.get('localid')

        central_key = (album_id, album_mbid)
        central_album_art = central_album_arts.get(central_key)

        # Identifica album con artwork locale
        if central_album_art:
            for art_url in central_album_art.values():
                if art_url and art_url.startswith('smb'):
                    id_albums_with_local_art.add(album_id)
                    break

        # Elabora se esiste corrispondenza locale
        if local_id:
            local_key = (local_id, album_mbid)
            local_album_art = local_album_arts.get(local_key)

            if album_id in id_albums_with_local_art:
                replace_local_art_with_artworker(album_id, db_params, central_album_art)

            _prepare_art_tuples_optimized(
                arts_to_insert, arts_to_remove, arts_to_update,
                central_album_art, local_album_art, local_id, 'album'
            )

    return {'insert': arts_to_insert, 'remove': arts_to_remove, 'update': arts_to_update}


def _process_song_batch(song_batch, central_song_arts, local_song_arts):
    """Elabora un batch di brani"""
    arts_to_insert = set()
    arts_to_remove = set()
    arts_to_update = set()

    for song in song_batch:
        local_id = song.get('localid')
        if not local_id:
            continue

        song_id = song.get('id')
        album_mbid = song.get('album_mbid')
        title = song.get('title')

        central_key = (song_id, album_mbid, title)
        local_key = (local_id, album_mbid, title)
        central_song_art = central_song_arts.get(central_key)
        local_song_art = local_song_arts.get(local_key)

        _prepare_art_tuples_optimized(
            arts_to_insert, arts_to_remove, arts_to_update,
            central_song_art, local_song_art, local_id, 'song'
        )

    return {'insert': arts_to_insert, 'remove': arts_to_remove, 'update': arts_to_update}


def _process_artists(central_artists_by_mbid, local_artists_by_mbid):
    """Elabora artisti"""
    arts_to_insert = set()
    arts_to_remove = set()
    arts_to_update = set()

    for central_mbid, central_artist_info in central_artists_by_mbid.items():
        local_artist_info = local_artists_by_mbid.get(central_mbid)

        if local_artist_info:
            central_artist_art = ({'thumb': central_artist_info.get('art_url')}
                                  if central_artist_info.get('art_url') else None)
            local_artist_art = ({'thumb': local_artist_info.get('art_url')}
                                if local_artist_info.get('art_url') else None)

            central_artist_info['id'] = local_artist_info.get('id')

            _prepare_art_tuples_optimized(
                arts_to_insert, arts_to_remove, arts_to_update,
                central_artist_art, local_artist_art, local_artist_info.get('id'), 'artist'
            )

    return {'insert': arts_to_insert, 'remove': arts_to_remove, 'update': arts_to_update}


def trigger_scan():
    paths_to_scan = []
    paths_to_scan_local = []
    exec_mode = db_scan.get_exec_mode()
    paths_from_params = db_scan.get_paths_from_params()
    db_params = db_scan.get_db_params()
    use_webdav = db_params.get('sourcetype') == 'webdav'
    if paths_from_params:
        for path in paths_from_params:
            if path not in paths_to_scan:
                paths_to_scan.append(path)
            if use_webdav:
                path = db_scan.convert_from_smb_to_davs(path)
            if path not in paths_to_scan_local:
                paths_to_scan_local.append(path)
    force_path_rescan(paths_to_scan_local)
    scan_folders(paths_to_scan_local)
    clean_paths()
    align_media_to_central_db(paths_to_scan, paths_to_scan_local, exec_mode, db_params)
    align_monitor = AlignMonitor()
    if align_monitor.wait_for_align():
        align_monitor.reset()
        builtin_cmd = f'NotifyAll({addon_id}, OnScanAndAlignFinished)'
        xbmc.executebuiltin(builtin_cmd)


if __name__ == '__main__':
    log(addon_name)
    trigger_scan()
