import json
import re
from urllib.parse import quote

import xbmc
import xbmcaddon
import xbmcgui
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


def get_arts():
    query = '''SELECT MEDIA_ID, MEDIA_TYPE, URL AS art_url FROM art WHERE TYPE LIKE 'thumb%' '''
    results = db_scan.connect_and_execute_to_local_db(db_scan.music_db, query)
    arts = [encode_to_kodi_image_url(result['art_url'], 'image://{0}/') for result in results if result['art_url'] is not None]
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


def clean_texture_cache():
    textures = get_textures()
    artworks = set(get_arts())
    id_textures_to_remove = [texture.get('textureid') for texture in textures if texture.get('url') not in artworks]
    if id_textures_to_remove:
        remove_textures(id_textures_to_remove)
        db_scan.compact_db(db_scan.texture_db)
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
