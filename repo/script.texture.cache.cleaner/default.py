import json
import re
import sqlite3
from urllib.parse import quote, unquote

import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs
import db_scan


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


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
    textures = json.loads(json_result).get('result').get('textures')
    return textures


def encode_to_kodi_image_url(art_url, img_url_prefix):
    encoded_url = art_url
    if not art_url.startswith('image://'):
        encoded_url = img_url_prefix.format(quote(art_url, '()!'))
        # Sostituisci manualmente il carattere `~` con la sua codifica
        encoded_url = encoded_url.replace('~', '%7e')
        encoded_url = re.sub(r'%[0-9A-F]{2}', lambda mo: mo.group().lower(), encoded_url)
    return encoded_url


def decode_url(path):
    return unquote(path)


# ottengo le potenziali texture usate come thumbnail quando si consultano le cartelle dalla vista per sorgenti (File su Kodi)
def get_files_thumbs(use_webdav):
    query = '''
        WITH file_info AS
          (SELECT strPath,
                  strFilename,
                  idSong
           FROM songview),
             ranked AS
          (SELECT strPath || strFilename AS full_path,
                  ROW_NUMBER() OVER (PARTITION BY strPath
                                     ORDER BY strFilename COLLATE NOCASE, idSong) AS row_num
           FROM file_info)
        SELECT full_path
        FROM ranked
        WHERE row_num = 1'''
    if use_webdav:
        query = '''
            WITH file_info AS
              (SELECT strPath,
                      strFilename,
                      decode(strFilename) AS decoded_filename,
                      idSong
               FROM songview),
                 ranked AS
              (SELECT strPath || strFilename AS full_path,
                      ROW_NUMBER() OVER (PARTITION BY strPath
                                         ORDER BY decoded_filename COLLATE NOCASE, idSong) AS row_num
               FROM file_info)
            SELECT full_path
            FROM ranked
            WHERE row_num = 1'''
    translated_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(translated_path)
    if use_webdav:
        music_db.create_function('decode', 1, decode_url, deterministic=True)
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    first_tracks_res = music_db_cursor.execute(query)
    results = first_tracks_res.fetchall()
    music_db_cursor.close()
    music_db.close()
    file_thumbs = [encode_to_kodi_image_url(full_path, 'image://music@{0}/') for (full_path,) in results]
    return file_thumbs


def get_arts():
    translated_path = db_scan.get_music_db_path()
    music_db = sqlite3.connect(translated_path)
    query = '''SELECT MEDIA_ID, MEDIA_TYPE, URL AS art_url FROM art WHERE TYPE LIKE 'thumb%' '''
    music_db.set_trace_callback(log)
    music_db_cursor = music_db.cursor()
    arts_res = music_db_cursor.execute(query)
    results = arts_res.fetchall()
    music_db_cursor.close()
    music_db.close()

    arts = [encode_to_kodi_image_url(art_url, 'image://{0}/') for (media_id, media_type, art_url) in results if art_url is not None]
    return arts


def remove_textures(id_textures):
    textures_requests = []
    for (id_rpc, id_texture) in enumerate(id_textures, 1):
        json_texture_remove_payload = {"jsonrpc": "2.0", "method": "Textures.RemoveTexture", "id": id_rpc,
                                       "params": {"textureid": id_texture}}
        if json_texture_remove_payload not in textures_requests:
            textures_requests.append(json_texture_remove_payload)
    json_result = xbmc.executeJSONRPC(json.dumps(textures_requests))
    json_result = json.loads(json_result)
    return json_result


# una bella compattata al db non guasta dopo tutto questo smarmellaio
def compact_db():
    db_path = db_scan.get_textures_db_path()
    db = sqlite3.connect(db_path)
    db.set_trace_callback(log)
    db_cursor = db.cursor()
    query = '''VACUUM'''
    db_cursor.execute(query)
    db_cursor.close()
    db.close()


def clean_texture_cache():
    db_params = db_scan.get_db_params()
    use_webdav = db_params.get('sourcetype') == 'webdav'
    textures = get_textures()
    artworks = set(get_arts())
    file_thumbs = set(get_files_thumbs(use_webdav))
    id_textures_to_remove = [texture.get('textureid') for texture in textures if texture.get('url') not in artworks and texture.get('url') not in file_thumbs]
    if id_textures_to_remove:
        remove_textures(id_textures_to_remove)
        compact_db()
        msg = f'Rimosse {len(id_textures_to_remove)} texture dalla cache locale'
        log(msg)
        dialog = xbmcgui.Dialog()
        icon_path = xbmcaddon.Addon().getAddonInfo('path') + '/' + 'icon.png'
        dialog.notification(addon_name, msg, icon_path)
    builtin_cmd = f'NotifyAll({addon_id}, OnTextureCacheCleaned)'
    xbmc.executebuiltin(builtin_cmd)


if __name__ == '__main__':
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    addon_id = xbmcaddon.Addon().getAddonInfo('id')
    log(addon_name)
    clean_texture_cache()
