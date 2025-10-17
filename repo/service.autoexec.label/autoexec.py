import xbmcaddon

from default import init_music_database, log

if __name__ == '__main__':
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)
    init_music_database()