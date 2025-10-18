import json
import os
import re
import string
import xml.etree.ElementTree as ET
from urllib.parse import quote
from xml.dom import minidom

import xbmc
import xbmcaddon
import xbmcvfs

alphabetical_artists_special_path = 'special://masterprofile/library/music/artistialfabetici/'
alphabetical_albums_special_path = 'special://masterprofile/library/music/albumalfabetici/'
alphabetical_compilations_special_path = 'special://masterprofile/library/music/compilationalfabetiche/'

local_alphabetical_artists = xbmcvfs.translatePath(alphabetical_artists_special_path)
local_alphabetical_albums = xbmcvfs.translatePath(alphabetical_albums_special_path)
local_alphabetical_compilations = xbmcvfs.translatePath(alphabetical_compilations_special_path)

artist_sort = 'artist'
album_sort = 'album'
artists_result = 'artists'
albums_result = 'albums'
artists_api_name = 'GetArtists'
albums_api_name = 'GetAlbums'
non_alphabetical_folder = '#0'

artists_default_icon = 'DefaultMusicArtists.png'
albums_default_icon = 'DefaultMusicAlbums.png'
compilations_default_icon = 'DefaultMusicCompilations.png'

compilation_folder_path = 'musicdb://compilations/?xsp='
artist_folder_path = 'musicdb://artists/?xsp='
album_folder_path = 'musicdb://albums/?compilation=false&xsp='


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def get_artists():
    artists_response = get_items(artists_api_name, False, artist_sort)
    artists = artists_response.get('result').get(artists_result)
    return artists


def get_albums():
    albums_response = get_items(albums_api_name, False, album_sort)
    albums = albums_response.get('result').get(albums_result)
    return albums


def get_compilations():
    compilations_response = get_items(albums_api_name, True, album_sort)
    compilations = compilations_response.get('result').get(albums_result)
    return compilations


# fa l'encoding di una stringa nel modo che piace a Kodi
# considerando lo slash come carattere da codificare a differenza di () e !
# inoltre i parametri di codifica degli uri devono essere in minuscolo
def encode_string(string_to_encode, safe_chars='()!'):
    encoded_string = re.sub(r'%[0-9A-F]{2}', lambda mo: mo.group().lower(), quote(string_to_encode, safe_chars))
    # Sostituisci manualmente il carattere `~` con la sua codifica
    encoded_string = encoded_string.replace('~', '%7e')
    return encoded_string


def get_items(api_name, fetch_compilations, sort_field):
    full_api_name = f'AudioLibrary.{api_name}'

    json_payload = {
        "jsonrpc": "2.0",
        "method": full_api_name,
        "id": "1",
        "params": {
            "properties": [],
            "sort": {
                "method": sort_field
            }
        }
    }

    if fetch_compilations:
        compilation_filter = {
            "and": [
                {
                    "field": "compilation",
                    "operator": "is",
                    "value": "true"
                }
            ]
        }
        json_params = json_payload.get('params')
        json_params['filter'] = compilation_filter
        json_payload['params'] = json_params

    response = xbmc.executeJSONRPC(json.dumps(json_payload))
    json_response = json.loads(response)
    return json_response


def init_node(folder_to_init, default_icon, main_node_label, order):
    xbmcvfs.mkdir(folder_to_init)
    file_name = 'index.xml'
    destination_path = os.path.join(folder_to_init, file_name)
    node = ET.Element('node', order=str(order))
    label = ET.SubElement(node, 'label')
    label.text = main_node_label
    icon = ET.SubElement(node, 'icon')
    icon.text = default_icon
    index_xml = minidom.parseString(ET.tostring(node, encoding='UTF-8')).toprettyxml()
    with xbmcvfs.File(destination_path, 'w') as genre_file:
        genre_file.write(index_xml)


def get_folder_path(base_path, values, sort_field, media_type):
    xsp_rule = {
        "order": {
            "direction": "ascending",
            "ignorefolders": 0,
            "method": sort_field
        },
        "rules": {
            "and": [
                {
                    "field": sort_field,
                    "operator": "startswith",
                    "value": values
                }
            ]
        },
        "type": media_type
    }
    json_xsp_rule = json.dumps(xsp_rule, separators=(',', ':'), ensure_ascii=False)
    encoded_xsp = encode_string(json_xsp_rule)
    path = f'{base_path}{encoded_xsp}'
    return path


def generate_folder_node(node_name, folder_path, order, destination):
    file_name = f'{node_name}.xml'
    destination_path = os.path.join(destination, file_name)
    node = ET.Element('node', order=str(order), type='folder')
    label = ET.SubElement(node, 'label')
    label.text = node_name.upper()
    path = ET.SubElement(node, 'path')
    path.text = folder_path
    node_xml = minidom.parseString(ET.tostring(node, encoding='UTF-8')).toprettyxml()
    with xbmcvfs.File(destination_path, 'w') as genre_file:
        genre_file.write(node_xml)


def generate_alphabetical_nodes(first_letters, node_path, musicdb_path, sort_field, media_type):
    alphabetic_letters = string.ascii_letters
    filtered_letters = sorted(list(filter(lambda letter: letter in alphabetic_letters, first_letters)))
    non_alpha_chars = sorted(list(filter(lambda letter: letter not in alphabetic_letters, first_letters)))

    for (order, filtered_letter) in enumerate(filtered_letters, start=2):
        low_filtered_letter = filtered_letter.lower()
        node_folder_path = get_folder_path(musicdb_path, [low_filtered_letter], sort_field, media_type)
        generate_folder_node(low_filtered_letter, node_folder_path, order, xbmcvfs.translatePath(node_path))

    non_alpha_folder_path = get_folder_path(musicdb_path, non_alpha_chars, sort_field, media_type)
    generate_folder_node(non_alphabetical_folder, non_alpha_folder_path, 1, xbmcvfs.translatePath(node_path))


def get_first_letters(medias):
    first_letters = []
    for media in medias:
        first_letter = media.get('label')[:1].lower()
        if first_letter not in first_letters:
            first_letters.append(first_letter)
    return first_letters


def preload_alphabetical_nodes():
    # inizializzo i vari nodi
    if not xbmcvfs.exists(local_alphabetical_artists):
        log(f'Inizializzo Artisti Alfabetici')
        init_node(local_alphabetical_artists, artists_default_icon, 'Artisti Alfabetici', 30)

    if not xbmcvfs.exists(local_alphabetical_albums):
        log(f'Inizializzo Album Alfabetici')
        init_node(local_alphabetical_albums, albums_default_icon, 'Album Alfabetici', 20)

    if not xbmcvfs.exists(local_alphabetical_compilations):
        log(f'Inizializzo Compilation Alfabetiche')
        init_node(local_alphabetical_compilations, compilations_default_icon, 'Compilation Alfabetiche', 60)

    artists = get_artists()
    albums = get_albums()
    compilations = get_compilations()

    artists_first_letters = get_first_letters(artists)
    albums_first_letters = get_first_letters(albums)
    compilations_first_letters = get_first_letters(compilations)

    generate_alphabetical_nodes(artists_first_letters, alphabetical_artists_special_path, artist_folder_path,
                                artist_sort, artists_result)
    generate_alphabetical_nodes(albums_first_letters, alphabetical_albums_special_path, album_folder_path, album_sort,
                                albums_result)
    generate_alphabetical_nodes(compilations_first_letters, alphabetical_compilations_special_path,
                                compilation_folder_path, album_sort, albums_result)


if __name__ == '__main__':
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    log(addon_name)
    preload_alphabetical_nodes()
