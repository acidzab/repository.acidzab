import os
import sys
from urllib.parse import quote

import db_scan
import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def encode_playlist_name(playlist_name):
    return quote(playlist_name, safe='()=!$,*+:@/&\'')


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
        media_title = f'{current_item.getMusicInfoTag().getTrack()}. {current_item.getMusicInfoTag().getArtist()} - {current_item.getMusicInfoTag().getTitle()}'
        track_length = current_item.getMusicInfoTag().getDuration()
        media_location = current_item.getPath()
        if use_webdav:
            media_location = db_scan.convert_from_davs_to_smb(current_item.getPath())
    media = '#EXTINF:{0},{1}\n{2}\n'.format(track_length, media_title, media_location)
    log('Title: {0}'.format(str(media_title)))
    log('Length in seconds: {0}'.format(str(track_length)))
    log('Media path {0}'.format(str(media_location)))
    main()
