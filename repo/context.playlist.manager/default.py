import os
from urllib.parse import quote

import xbmc
import xbmcgui
import xbmcvfs


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def encode_playlist_name(playlist_name):
    return quote(playlist_name, safe='()=!$,*+:@/&\'')


def upload_to_central_directory(playlist_path, db_params, is_playlist_deletable):
    use_webdav = db_params.get('sourcetype') == 'webdav'
    filename = playlist_path.split(os.sep)[-1]
    central_playlist_path = f'{db_params.get('sambasource')}/playlists/music/{filename}'
    if use_webdav:
        # porkaround: non passo da webdav per scrivere ma da sftp
        writing_source = db_params.get('webdavsource').replace('davs', 'sftp')
        central_playlist_path = f'{writing_source}/playlists/music/{filename}'
    if is_playlist_deletable:
        xbmcvfs.delete(playlist_path)
        xbmcvfs.delete(central_playlist_path)
    else:
        xbmcvfs.copy(playlist_path, central_playlist_path)


def remove_from_playlist(media_to_remove, db_params):
    upload_to_central = db_params.get('centralplaylist')
    lines_to_remove = media_to_remove.split('\n')
    # porkaround tremendo: cancello anche con un solo brano dentro
    lines_with_one_track = 3
    is_playlist_deletable = False
    folder_path = xbmcvfs.translatePath(xbmc.getInfoLabel('Container.FolderPath'))
    with xbmcvfs.File(folder_path, 'r') as fr:
        lines = fr.read().split('\n')
    with xbmcvfs.File(folder_path, 'w') as fw:
        written_lines = 0
        for line in lines:
            if line.strip('\n') not in lines_to_remove:
                fw.write(line + '\n')
                written_lines += 1
        if written_lines == lines_with_one_track:
            is_playlist_deletable = True

    if upload_to_central:
        upload_to_central_directory(folder_path, db_params, is_playlist_deletable)


def write_playlist(folder, playlist, media, mode, db_params):
    upload_to_central = db_params.get('centralplaylist')
    if not folder:
        folder_list = ['music']
        result = xbmcgui.Dialog().select('Seleziona la tipologia di playlist', folder_list)
        if result > -1:
            folder = folder_list[result]
    log('Folder: {0}'.format(folder))
    if mode == 'w':
        playlist_name = xbmcgui.Dialog().input('Inserisci il nome della playlist da creare',
                                               type=xbmcgui.INPUT_ALPHANUM)
        if len(playlist_name) > 1:
            playlist = xbmcvfs.translatePath(os.path.join('special://profile/playlists/{0}'.format(folder), '{0}.m3u8'
                                                          .format(playlist_name)))
        else:
            return None
        media = '#EXTM3U\n{0}'.format(media)
    else:
        playlist = xbmcvfs.translatePath(os.path.join('special://profile/playlists/{0}'.format(folder), playlist))
    log('Path: {0}'.format(playlist))
    playlist_content = None
    if mode == 'a':
        # simulo un append
        playlist_content = read_playlist(playlist)
        if media not in playlist_content:
            playlist_content += media
    try:
        with xbmcvfs.File(playlist, 'w') as f:
            if mode == 'a' and playlist_content is not None:
                f.write(playlist_content)
                log('Added: {0}'.format(playlist_content))
            else:
                f.write(media)
                log('Added: {0}'.format(media))
            if upload_to_central:
                upload_to_central_directory(playlist, db_params, False)
        return True
    except Exception as e:
        log('Error: {0}'.format(e))
        return False


def read_playlist(playlist_path):
    with xbmcvfs.File(playlist_path) as f:
        playlist_content = f.read()
    return playlist_content


def filter_playlist(playlists):
    basic_playlists = []
    for p in playlists:
        is_playlist = p.find('.m3u8') > 0 or p.find('.m3u') > 0
        if is_playlist:
            basic_playlists.append(p)
    return sorted(basic_playlists)
