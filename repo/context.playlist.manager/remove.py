import sys

import db_scan
import xbmc
import xbmcaddon
import xbmcgui

from default import log, remove_from_playlist


def main():
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
        media_location = current_item.getPath()
        if use_webdav:
            media_location = db_scan.convert_from_davs_to_smb(current_item.getPath())
    media = '#EXTINF:{0},{1}\n{2}\n'.format(track_length, media_title, media_location)
    log('Title: {0}'.format(str(media_title)))
    log('Length in seconds: {0}'.format(str(track_length)))
    log('Media path {0}'.format(str(media_location)))
    # Chiedi conferma all'utente
    if xbmcgui.Dialog().yesno("Rimuovi dalla playlist", "Vuoi rimuovere il brano dalla playlist ?"):
        remove_from_playlist(media, db_params)
        xbmc.executebuiltin('Container.Refresh')
        icon_path = xbmcaddon.Addon().getAddonInfo('path') + '/' + 'icon.png'
        xbmcgui.Dialog().notification(addon_name, 'Rimossa: {0}'.format(media_title), icon_path, 5000)


if __name__ == '__main__':
    main()
