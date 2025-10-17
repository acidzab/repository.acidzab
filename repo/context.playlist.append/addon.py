import os
import sys
from urllib.parse import unquote

import db_scan
import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def write_playlist(folder, playlist, media, mode):
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
            return
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


def main():
    playlist_folder = playlists['folder']
    status = None
    if len(basic_playlists) > 0:
        basic_playlists.insert(0, 'Nuova playlist')
        result = xbmcgui.Dialog().select('Seleziona una playlist', basic_playlists)
        if result > -1:
            log('Playlist: {0}'.format(basic_playlists[result]))
            if result == 0:
                status = write_playlist(playlist_folder, None, media, 'w')
            else:
                if not playlists['folder']:
                    playlist_folder = basic_playlists[result][1:6]
                    playlist = basic_playlists[result][8:len(basic_playlists[result])]
                else:
                    playlist = basic_playlists[result]
                status = write_playlist(playlist_folder, playlist, media, 'a')
    else:
        status = write_playlist(playlist_folder, None, media, 'w')
    if status:
        icon_path = xbmcaddon.Addon().getAddonInfo('path') + '/' + 'icon.png'
        xbmcgui.Dialog().notification(addon_name, 'Added: {0}'.format(media_title), icon_path, 5000)
    elif status is not None and status is False:
        xbmcgui.Dialog().notification(addon_name, 'Failed: {0}'.format(media_title), xbmcgui.NOTIFICATION_ERROR, 5000)


if __name__ == '__main__':
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)
    db_params = db_scan.get_db_params()
    use_webdav = db_params.get('sourcetype') == 'webdav'
    basic_playlists = []
    playlist_folder = False
    playlists = {}
    media_title = None
    track_length = 0
    media_location = None
    log('Translated path: {0}'.format(os.path.join('special://profile/playlists/music')))
    playlists.update({'music': xbmcvfs.listdir(os.path.join('special://profile/playlists/music'))[1]})
    if xbmc.getCondVisibility('Container.Content(songs)') == 1:
        log('Container: songs')
        playlists.update({'folder': 'music'})
        current_item = sys.listitem
        media_title = current_item.getLabel()
        basic_playlists = filter_playlist(playlists['music'])
        track_length = current_item.getMusicInfoTag().getDuration()
        media_location = current_item.getMusicInfoTag().getURL()
        if use_webdav:
            media_location = db_scan.convert_from_davs_to_smb(current_item.getMusicInfoTag().getURL())
    media = '#EXTINF:{0},{1}\n{2}\n'.format(track_length, media_title, media_location)
    log('Title: {0}'.format(str(media_title)))
    log('Length in seconds: {0}'.format(str(track_length)))
    log('Playlists: {0}'.format(str(basic_playlists)))
    main()
