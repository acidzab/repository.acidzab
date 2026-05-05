import os
import sys

import db_scan
import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

from default import log, filter_playlist, write_playlist


def main():
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)
    db_params = db_scan.get_db_params()
    use_webdav = db_params.get('sourcetype') == 'webdav'
    basic_playlists = []
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
        track_number = '{:02}'.format(current_item.getMusicInfoTag().getTrack())
        media_title = f'{track_number}. {current_item.getMusicInfoTag().getArtist()} - {current_item.getMusicInfoTag().getTitle()}'
        basic_playlists = filter_playlist(playlists['music'])
        track_length = current_item.getMusicInfoTag().getDuration()
        media_location = current_item.getMusicInfoTag().getURL()
        if use_webdav:
            media_location = db_scan.convert_from_davs_to_smb(current_item.getMusicInfoTag().getURL())
    media = '#EXTINF:{0},{1}\n{2}\n'.format(track_length, media_title, media_location)
    log('Title: {0}'.format(str(media_title)))
    log('Length in seconds: {0}'.format(str(track_length)))
    log('Playlists: {0}'.format(str(basic_playlists)))
    playlist_folder = playlists['folder']
    status = None
    if len(basic_playlists) > 0:
        basic_playlists.insert(0, 'Nuova playlist')
        result = xbmcgui.Dialog().select('Seleziona una playlist', basic_playlists)
        if result > -1:
            log('Playlist: {0}'.format(basic_playlists[result]))
            if result == 0:
                status = write_playlist(playlist_folder, None, media, 'w', db_params)
            else:
                if not playlists['folder']:
                    playlist_folder = basic_playlists[result][1:6]
                    playlist = basic_playlists[result][8:len(basic_playlists[result])]
                else:
                    playlist = basic_playlists[result]
                status = write_playlist(playlist_folder, playlist, media, 'a', db_params)
    else:
        status = write_playlist(playlist_folder, None, media, 'w', db_params)
    if status:
        icon_path = xbmcaddon.Addon().getAddonInfo('path') + '/' + 'icon.png'
        xbmcgui.Dialog().notification(addon_name, 'Added: {0}'.format(media_title), icon_path, 5000)
    elif status is not None and status is False:
        xbmcgui.Dialog().notification(addon_name, 'Failed: {0}'.format(media_title), xbmcgui.NOTIFICATION_ERROR, 5000)


if __name__ == '__main__':
    main()
