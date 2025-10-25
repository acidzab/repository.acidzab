import os
import sys

import db_scan
import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def remove_from_playlist(media_to_remove, db_params):
    upload_to_central = db_params.get('centralplaylist')
    lines_to_remove = media_to_remove.split('\n')
    folder_path = xbmcvfs.translatePath(xbmc.getInfoLabel('Container.FolderPath'))
    with xbmcvfs.File(folder_path, 'r') as fr:
        lines = fr.read().split('\n')
    with xbmcvfs.File(folder_path, 'w') as fw:
        for line in lines:
            if line.strip('\n') not in lines_to_remove:
                fw.write(line + '\n')
    if upload_to_central:
        upload_to_central_directory(folder_path, db_params)


def upload_to_central_directory(playlist_path, db_params):
    use_webdav = db_params.get('sourcetype') == 'webdav'
    filename = playlist_path.split(os.sep)[-1]
    central_directory = f'{db_params.get('webdavsource')}/playlists/music/{filename}' if use_webdav else f'{db_params.get('sambasource')}/playlists/music/{filename}'
    xbmcvfs.copy(playlist_path, central_directory)


def main():
    # Chiedi conferma all'utente
    if xbmcgui.Dialog().yesno("Rimuovi dalla playlist", "Vuoi rimuovere il brano dalla playlist ?"):
        remove_from_playlist(media, db_params)
        xbmc.executebuiltin('Container.Refresh')
        icon_path = xbmcaddon.Addon().getAddonInfo('path') + '/' + 'icon.png'
        xbmcgui.Dialog().notification(addon_name, 'Rimossa: {0}'.format(media_title), icon_path, 5000)


if __name__ == '__main__':
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)
    db_params = db_scan.get_db_params()
    use_webdav = db_params.get('sourcetype') == 'webdav'
    media_title = None
    track_length = 0
    if xbmc.getCondVisibility('Container.Content(songs)') == 1:
        log('Container: songs')
        current_item = sys.listitem
        media_title = current_item.getLabel()
        track_length = current_item.getMusicInfoTag().getDuration()
        media_location = current_item.getMusicInfoTag().getURL()
        if use_webdav:
            media_location = db_scan.convert_from_davs_to_smb(current_item.getMusicInfoTag().getURL())
    media = '#EXTINF:{0},{1}\n{2}\n'.format(track_length, media_title, media_location)
    log('Title: {0}'.format(str(media_title)))
    log('Length in seconds: {0}'.format(str(track_length)))
    main()
