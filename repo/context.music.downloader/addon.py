import json
import sys
from urllib.parse import unquote

import db_scan
import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

db_params = db_scan.get_db_params()


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def get_songs_by_albums(id_album):
    album_songs = []
    get_payload = {
        "jsonrpc": "2.0",
        "method": "AudioLibrary.GetSongs",
        "id": "1",
        "params": {
            "properties": [
                "file"
            ],
            "filter": {
                "albumid": int(id_album)
            }
        }
    }
    json_result = xbmc.executeJSONRPC(json.dumps(get_payload))
    json_song = json.loads(json_result)
    json_result = json_song.get('result')
    if json_result:
        songs = json_result.get('songs')
        if songs:
            for song in songs:
                if song.get('file') and song.get('file') not in album_songs:
                    album_songs.append(song.get('file'))
    return album_songs


def get_directory(smb_path):
    json_get_directory_payload = {
        "jsonrpc": "2.0",
        "method": "Files.GetDirectory",
        "id": "1",
        "params": {
            "directory": smb_path
        }
    }
    get_directory_req = xbmc.executeJSONRPC(json.dumps(json_get_directory_payload))
    response = json.loads(get_directory_req)
    return response


def get_songs_by_playlist(playlist):
    playlist_songs = []
    if playlist:
        get_directory_res = get_directory(playlist)
        if get_directory_res:
            directories = get_directory_res.get('result').get('files')
            for directory in directories:
                if directory.get('filetype') == 'file' and directory.get('file') not in playlist_songs:
                    playlist_songs.append(directory.get('file'))
    return playlist_songs


def download_files(paths, folder_name, use_webdav):
    base_path = f'special://masterprofile/library/music'
    if db_params.get('downloadfolder'):
        base_path = db_params.get('downloadfolder')
    special_path = f'{base_path}/{folder_name}/'
    dl_path = xbmcvfs.makeLegalFilename(special_path)
    if not xbmcvfs.exists(dl_path):
        xbmcvfs.mkdir(dl_path)

    progress = xbmcgui.DialogProgressBG()
    initial_message = unquote(paths[0].split('/')[len(paths[0].split('/')) - 1]) if use_webdav else paths[0].split('/')[
        len(paths[0].split('/')) - 1]
    progress.create(addon_name, initial_message)
    total_songs_to_process = len(paths)
    for i in range(len(paths)):
        step = i + 1
        file_path = paths[i]
        splitted_path = file_path.split('/')
        splitted_path_msg = splitted_path
        if i < len(paths) - 1:
            splitted_path_msg = paths[i + 1].split('/')
        file_name = unquote(splitted_path[len(splitted_path) - 1]) if use_webdav else splitted_path[len(splitted_path) - 1]
        file_name_msg = unquote(splitted_path_msg[len(splitted_path_msg) - 1]) if use_webdav else splitted_path_msg[len(splitted_path_msg) - 1]
        destination_path = dl_path + file_name
        destination_path = xbmcvfs.makeLegalFilename(destination_path)
        if not xbmcvfs.exists(destination_path):
            xbmcvfs.copy(file_path, destination_path)
        percentuale = (step / total_songs_to_process) * 100
        progress.update(percent=int(percentuale), message=file_name_msg)

    progress.close()


def main(current_item):
    use_webdav = db_params.get('sourcetype') == 'webdav'
    if xbmc.getCondVisibility('Container.Content(songs)') == 1 or xbmc.getCondVisibility(
            'Container.Content(albums)') == 1:
        media_id = current_item.getMusicInfoTag().getDbId()
        album_name = current_item.getMusicInfoTag().getAlbum()
        album_artist = current_item.getMusicInfoTag().getAlbumArtist()
        album_year = current_item.getMusicInfoTag().getYear()

        folder_name = f'{album_artist} - {album_name} ({album_year})'
    else:
        # riutilizzo come nome cartella quello della playlist
        playlist_name = unquote(current_item.getLabel()) if use_webdav else current_item.getLabel()
        folder_name = playlist_name.replace('.m3u8', '')
    files_to_download = []
    if xbmc.getCondVisibility('Container.Content(songs)') == 1:
        files_to_download.append(current_item.getMusicInfoTag().getURL())
    elif xbmc.getCondVisibility('Container.Content(albums)') == 1:
        files_to_download.extend(get_songs_by_albums(media_id))
    elif xbmc.getCondVisibility('String.Contains(Container.FolderPath,playlists)') == 1:
        files_to_download.extend(get_songs_by_playlist(current_item.getPath()))
    if files_to_download:
        download_files(files_to_download, folder_name, use_webdav)
        icon_path = xbmcaddon.Addon().getAddonInfo('path') + '/' + 'icon.png'
        if xbmc.getCondVisibility('String.Contains(Container.FolderPath,playlists)') == 1:
            media_title = folder_name
        else:
            media_title = current_item.getLabel()
        xbmcgui.Dialog().notification(addon_name, '{0} scaricato con successo!'.format(media_title), icon_path, 5000)


if __name__ == '__main__':
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)
    current_item = sys.listitem
    main(current_item)
