import json
import os.path
import re
import xml.etree.ElementTree as ET
from urllib.parse import quote
from xml.dom import minidom

import xbmc
import xbmcaddon
import xbmcvfs

genres_local_special = 'special://masterprofile/library/music/genres/'
genres_local = xbmcvfs.translatePath(genres_local_special)
kodi_genre_default_icon = 'DefaultMusicGenres.png'


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def get_genres():
    json_payload = {
        "jsonrpc": "2.0",
        "method": "AudioLibrary.GetGenres",
        "id": "1",
        "params": {
            "properties": [],
            "sort": {
                "method": "genre"
            }
        }
    }
    response = xbmc.executeJSONRPC(json.dumps(json_payload))
    json_response = json.loads(response)
    genres = json_response.get('result').get('genres')
    return genres


# fa l'encoding di una stringa nel modo che piace a Kodi
# considerando lo slash come carattere da codificare a differenza di () e !
# inoltre i parametri di codifica degli uri devono essere in minuscolo
def encode_string(string_to_encode, safe_chars='()!'):
    encoded_string = re.sub(r'%[0-9A-F]{2}', lambda mo: mo.group().lower(), quote(string_to_encode, safe_chars))
    # Sostituisci manualmente il carattere `~` con la sua codifica
    encoded_string = encoded_string.replace('~', '%7e')
    return encoded_string


def generate_genres_folder_node(genre, order, filename_by_genre):
    genre_name = genre.get('label')
    file_name = filename_by_genre.get(genre_name)
    destination_path = os.path.join(genres_local, file_name)
    xsp_rule = {
        "order": {
            "direction": "ascending",
            "ignorefolders": 0,
            "method": "album"
        },
        "rules": {
            "and": [
                {
                    "field": "genre",
                    "operator": "is",
                    "value": [
                        genre_name
                    ]
                }
            ]
        },
        "type": "albums"
    }
    if not xbmcvfs.exists(destination_path):
        node = ET.Element('node', order=str(order), type='folder', visible='Library.HasContent(Music)')
        label = ET.SubElement(node, 'label')
        label.text = genre_name
        icon = ET.SubElement(node, 'icon')
        icon.text = kodi_genre_default_icon
        json_xsp_rule = json.dumps(xsp_rule, separators=(',', ':'), ensure_ascii=False)
        encoded_xsp = encode_string(json_xsp_rule)
        folder_path = f'musicdb://albums/?xsp={encoded_xsp}'
        path = ET.SubElement(node, 'path')
        path.text = folder_path
        genre_xml = minidom.parseString(ET.tostring(node, encoding='UTF-8')).toprettyxml()
        with xbmcvfs.File(destination_path, 'w') as genre_file:
            genre_file.write(genre_xml)


def init_genres_node():
    xbmcvfs.mkdir(genres_local)
    file_name = 'index.xml'
    destination_path = os.path.join(genres_local, file_name)
    node = ET.Element('node', order='70')
    label = ET.SubElement(node, 'label')
    label.text = 'Generi'
    icon = ET.SubElement(node, 'icon')
    icon.text = kodi_genre_default_icon
    index_xml = minidom.parseString(ET.tostring(node, encoding='UTF-8')).toprettyxml()
    with xbmcvfs.File(destination_path, 'w') as genre_file:
        genre_file.write(index_xml)


def get_filename_by_genre(genres):
    filename_by_genre = {}
    for genre in genres:
        genre_name = genre.get('label')
        file_name = re.sub(r"(\W+)", '', genre_name.lower(), flags=re.MULTILINE)
        filename_by_genre[genre_name] = file_name + '.xml'
    return filename_by_genre


def preload_genres():
    # inizializzo generi e il suo relativo nodo
    if not xbmcvfs.exists(genres_local):
        log(f'Inizializzo Generi')
        init_genres_node()

    genres = get_genres()
    filename_by_genre = get_filename_by_genre(genres)
    for (order, genre) in enumerate(genres, 1):
        generate_genres_folder_node(genre, order, filename_by_genre)

    dirs, files = xbmcvfs.listdir(genres_local)
    potential_nodes = filename_by_genre.values()
    for file in files:
        if file not in potential_nodes and file != 'index.xml':
            path_to_delete = os.path.join(genres_local, file)
            xbmcvfs.delete(path_to_delete)


if __name__ == '__main__':
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)
    preload_genres()
