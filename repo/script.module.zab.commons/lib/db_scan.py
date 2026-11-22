import json
import re
import sqlite3
import sys
from urllib.parse import unquote, quote, parse_qs

import requests
import xbmc
import xbmcvfs
from requests import auth

kodi_local_db_path = xbmcvfs.translatePath('special://userdata/Database/')
view_modes_db = 'viewmodes'
texture_db = 'textures'
music_db = 'music'
addons_db = 'addons'
video_db = 'videos'
epg_db = 'epg'
tv_db = 'tv'

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


def execute_from_central_kodi_webserver(db_params, payload):
    headers = {
        'content-type': 'application/json;'
    }
    central_kodi_host = db_params.get('serverhost')
    central_kodi_server_user = db_params.get('rpcserveruser')
    central_kodi_server_password = db_params.get('rpcserverpass')
    basic_auth = auth.HTTPBasicAuth(central_kodi_server_user, central_kodi_server_password)
    kodi_instance = f'{central_kodi_host}'
    response = requests.post(kodi_instance, headers=headers, json=payload, auth=basic_auth)
    response.raise_for_status()
    return response.json()


def execute_from_db_gateway(db_params, api_path, http_verb, payload=None, query_params=None):
    headers = {
        'content-type': 'application/json;'
    }
    db_gateway_host = db_params.get('dbgateway')
    db_gateway_user = db_params.get('dbgatewayuser')
    db_gateway_password = db_params.get('dbgatewaypass')
    basic_auth = auth.HTTPBasicAuth(db_gateway_user, db_gateway_password)
    api_to_call = f'{db_gateway_host}{api_path}'
    if http_verb == 'GET':
        response = requests.get(api_to_call, headers=headers, auth=basic_auth, params=query_params)
        response.raise_for_status()
        return response.json()
    elif http_verb == 'POST':
        response = requests.post(api_to_call, headers=headers, json=payload, auth=basic_auth, params=query_params)
        response.raise_for_status()
        return response.json()
    elif http_verb == 'DELETE':
        response = requests.delete(api_to_call, headers=headers, json=payload, auth=basic_auth, params=query_params)
        response.raise_for_status()
        return response.json()
    return None


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
    results = {}
    results[addons_db] = xbmc.getDatabaseName(addons_db)
    results[epg_db] = xbmc.getDatabaseName(epg_db)
    results[music_db] = xbmc.getDatabaseName(music_db)
    results[texture_db] = xbmc.getDatabaseName(texture_db)
    results[tv_db] = xbmc.getDatabaseName(tv_db)
    results[video_db] = xbmc.getDatabaseName(video_db)
    results[view_modes_db] = xbmc.getDatabaseName(view_modes_db)
    return results


def get_db_path(db_name):
    db_versions = get_latest_kodi_dbs()
    db_name = db_versions.get(db_name)
    db_path = f'special://userdata/Database/{db_name}.db'
    return xbmcvfs.translatePath(db_path)


def connect_and_execute_to_local_db(db_type, query, params=None, execute_many=False):
    pragma_synchronous = 'PRAGMA synchronous=NORMAL;'
    db_path = get_db_path(db_type)
    sqlite_db = sqlite3.connect(db_path)
    sqlite_db.row_factory = sqlite3.Row
    sqlite_db.set_trace_callback(log)
    sqlite_db_cursor = sqlite_db.cursor()
    sqlite_db_cursor.execute(pragma_synchronous)
    if execute_many:
        sqlite_db_cursor.executemany(query, params)
    else:
        sqlite_db_cursor.execute(query, params)
    sqlite_db.commit()
    results = sqlite_db_cursor.fetchall()
    sqlite_db_cursor.close()
    sqlite_db.close()
    return results


def configure_local_dbs():
    kodi_dbs = get_latest_kodi_dbs()
    pragma_wal_query = 'PRAGMA journal_mode=WAL;'
    for db_type in kodi_dbs:
        db_path = get_db_path(db_type)
        sqlite_db = sqlite3.connect(db_path)
        sqlite_db.row_factory = sqlite3.Row
        sqlite_db.set_trace_callback(log)
        sqlite_db_cursor = sqlite_db.cursor()
        sqlite_db_cursor.execute(pragma_wal_query)
        sqlite_db.commit()
        sqlite_db_cursor.close()
        sqlite_db.close()


# una bella compattata al db non guasta dopo tutto questo smarmellaio
def compact_db(db_type):
    db_path = get_db_path(db_type)
    db = sqlite3.connect(db_path)
    db.set_trace_callback(log)
    db_cursor = db.cursor()
    query = '''VACUUM'''
    db_cursor.execute(query)
    db.commit()
    db_cursor.close()
    db.close()
