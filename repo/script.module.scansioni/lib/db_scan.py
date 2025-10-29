import json
import os
import re
import sys
from urllib.parse import unquote, quote, parse_qs

import pymysql.cursors
import requests
import xbmc
import xbmcvfs
from requests import auth

kodi_local_db_path = xbmcvfs.translatePath('special://userdata/Database/')


def log(msg):
    xbmc.log(str(msg), xbmc.LOGDEBUG)


def convert_from_davs_to_smb(davs_path):
    db_params = get_db_params()
    smb_source_base = db_params.get('sambasource')
    webdav_source_base = db_params.get('webdavsource')
    unquoted_davs = unquote(davs_path)
    smb_path = unquoted_davs.replace(webdav_source_base, smb_source_base)
    return smb_path


# Funzione per dividere in chunk
def split_json(data, max_size=40960):
    chunks = []
    current_chunk = []
    current_size = 0

    for item in data:
        item_size = len(json.dumps(item).encode('utf-8'))
        if current_size + item_size >= max_size:
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0
        current_chunk.append(item)
        current_size += item_size

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


# fa l'encoding di una stringa nel modo che piace a Kodi
# considerando lo slash come carattere da codificare a differenza di () e !
# inoltre i parametri di codifica degli uri devono essere in minuscolo
def encode_string(string_to_encode, safe_chars='()=!$,*+:@/&\''):
    encoded_string = re.sub(r'%[0-9A-F]{2}', lambda mo: mo.group().lower(), quote(string_to_encode, safe_chars))
    # Sostituisci manualmente il carattere `~` con la sua codifica
    # encoded_string = encoded_string.replace('~', '%7e')
    return encoded_string


def convert_from_smb_to_davs(smb_path):
    db_params = get_db_params()
    smb_source_base = db_params.get('sambasource')
    webdav_source_base = db_params.get('webdavsource')
    path_without_prefix = smb_path.replace(smb_source_base, '')
    dav_path = encode_string(path_without_prefix)
    dav_path = f'{webdav_source_base}{dav_path}'
    return dav_path


def read_params():
    # Leggi i parametri passati
    parsed_params = {}
    params_string = sys.argv[1] if len(sys.argv) > 1 else None
    if params_string:
        # Estrai i parametri dalla query string
        # devo fare doppia codifica sulle + perchè parse_qs me le traduce poi come spazi
        params_string = params_string.replace('%2b', encode_string('%2b', safe_chars='()!'))
        params_string = unquote(params_string)
        parsed_params = parse_qs(params_string.lstrip('?'), separator=';')
    return parsed_params


def get_exec_mode():
    # exec mode possono essere o scan o init, scanner trigger prevede anche align
    exec_mode = None
    parsed_params = read_params()
    if parsed_params and parsed_params.get('mode'):
        exec_mode = parsed_params.get('mode')[0]
    return exec_mode


def get_paths_from_params():
    paths_from_params = []
    parsed_params = read_params()
    if parsed_params and parsed_params.get('path'):
        paths_from_params = parsed_params.get('path')
    return paths_from_params


def get_jsons_to_process(db_params):
    # ogni client ha la sua tabella dedicata che funge da coda delle scansioni da processare
    # una volta processata la scansione si rimuove dalla coda

    scans = []
    ids_to_remove = []
    host = db_params.get('host')
    username = db_params.get('user')
    password = db_params.get('pass')
    table = db_params.get('table')
    query = '''SELECT * FROM %s WHERE content_scan IS NOT NULL''' % table
    central_db = pymysql.connect(host=host, user=username, password=password, database='scansioni', port=3306,
                                 cursorclass=pymysql.cursors.DictCursor, connect_timeout=18000)
    with central_db:
        with central_db.cursor() as central_cursor:
            central_cursor.execute(query)
            results = central_cursor.fetchall()
            for result in results:
                albums_from_json = json.loads(result.get('content_scan'))
                scans.append(albums_from_json)
                ids_to_remove.append(result.get('id'))
            if ids_to_remove:
                for id_to_remove in ids_to_remove:
                    delete_query = '''DELETE FROM {} WHERE id = %s '''.format(table)
                    central_cursor.execute(delete_query, (id_to_remove,))
                    central_db.commit()
    return scans


def execute_from_central_kodi_webserver(db_params, payload):
    headers = {
        'content-type': 'application/json;',
    }
    central_kodi_host = db_params.get('serverhost')
    central_kodi_server_port = db_params.get('rpcserverport')
    central_kodi_server_user = db_params.get('rpcserveruser')
    central_kodi_server_password = db_params.get('rpcserverpass')
    basic_auth = auth.HTTPBasicAuth(central_kodi_server_user, central_kodi_server_password)
    kodi_instance = f'http://{central_kodi_host}:{central_kodi_server_port}/jsonrpc'
    response = requests.post(kodi_instance, headers=headers, json=payload, auth=basic_auth)
    response.raise_for_status()
    return response.json()


def get_manual_arts_from_artworker(db_params, id_album):
    artworker_host = db_params.get('artworker')
    artworker_art_endpoint = f'{artworker_host}/albums/{id_album}/manual-arts'
    response = requests.get(artworker_art_endpoint)
    response.raise_for_status()
    return response.json()


def get_db_params():
    central_settings_path = xbmcvfs.translatePath('special://userdata/centralsettings.json')
    with xbmcvfs.File(central_settings_path) as f:
        central_settings = json.load(f)
    return central_settings


def get_latest_kodi_dbs():
    """
    Restituisce i database più aggiornati di Kodi presenti nella cartella Database.

    :return: dict con {Db: Nome db aggiornato}
    """
    # prefissi noti dei database Kodi
    prefixes = ["Addons", "Epg", "MyMusic", "MyVideos", "Textures", "TV", "ViewModes"]
    results = {}

    for prefix in prefixes:
        db_files = [f for f in os.listdir(kodi_local_db_path) if f.startswith(prefix) and f.endswith(".db")]
        if not db_files:
            continue

        # estrai numero finale e ordina
        db_files.sort(
            key=lambda x: int(re.search(r"(\d+)\.db$", x).group(1)),
            reverse=True
        )
        results[prefix] = db_files[0].replace(".db", "")

    return results


def get_db_path(db_name):
    db_versions = get_latest_kodi_dbs()
    db_name = db_versions.get(db_name)
    db_path = f'special://userdata/Database/{db_name}.db'
    return xbmcvfs.translatePath(db_path)


def get_music_db_path():
    return get_db_path('MyMusic')


def get_textures_db_path():
    return get_db_path('Textures')


def get_view_modes_db_path():
    return get_db_path('ViewModes')
