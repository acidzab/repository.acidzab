import json
import os
import re
import sqlite3
import string
import xml.etree.ElementTree as ET
from urllib.parse import quote, unquote
from xml.dom import minidom

import db_scan
import unicodedata
import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs
from unidecode import unidecode

db_params = db_scan.get_db_params()
smb_base_source = db_params.get('sambasource')
dav_base_source = db_params.get('webdavsource')
central_etichette_path = f'{dav_base_source}/etichette/' if db_params.get(
    'sourcetype') == 'webdav' else f'{smb_base_source}/etichette/'
etichette_local_special = 'special://masterprofile/library/music/etichette/'
etichette_local = xbmcvfs.translatePath(etichette_local_special)
kodi_label_default_icon = 'DefaultMusicAlbums.png'
main_node_label = 'Etichette Discografiche'
main_node_icon = 'DefaultAddonAlbumInfo.png'
folder_node_icon = 'DefaultFolder.png'
main_node_order = 40
smb_safe_chars = '()!'
non_alphabetical_folder = 'NSP'
allowed_files = ('.jpg', '.jpeg', '.png')


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
                "field": "url",
                "operator": "contains",
                "value": "etichette"
            }
        }
    }
    json_result = xbmc.executeJSONRPC(json.dumps(json_get_texture_payload))
    textures_res = json.loads(json_result).get('result').get('textures')
    return textures_res


def get_textures_urls(textures):
    textures_urls = []
    for texture in textures:
        textures_urls.append(texture.get('url'))
    return textures_urls


def sanitize(original_string, pattern, replace=""):
    sanitized_string = re.sub(pattern, replace, original_string)
    final_sanitized_string = ''.join(c for c in sanitized_string if unicodedata.category(c)[0] != 'C')
    return final_sanitized_string


def get_label_filename(label):
    return sanitize(label, r"[^\w\s\-]").lower().replace(' ', '')


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


def get_label_folder_name(label):
    label_folder_name = None
    if label:
        label_folder_name = label[0].capitalize()
        '''
        Lo -> altri caratteri
        Ll -> minuscolo
        Lu -> maiuscolo
        cambio di approccio: controllo che appartenga alle categorie unicode N*, S*, P* (Numberi, Simboli e Punteggiatura)
        '''
        unicode_category = unicodedata.category(label_folder_name)
        is_special_numeric = (unicode_category.startswith('N') or unicode_category.startswith('S')
                              or unicode_category.startswith('P'))
        if label_folder_name in string.punctuation or label_folder_name in string.digits or is_special_numeric:
            label_folder_name = non_alphabetical_folder
        label_folder_name = unidecode(label_folder_name)
        if label_folder_name != non_alphabetical_folder and len(label_folder_name) > 1:
            label_folder_name = label_folder_name[0].capitalize()
    return label_folder_name


def get_labels_dirs(id_albums, labels_to_process):
    label_dirs = []
    if id_albums and not labels_to_process:
        labels_to_process = get_labels_to_process(id_albums)
    if labels_to_process:
        for label_to_process in labels_to_process:
            folder_name = get_label_folder_name(label_to_process)
            if folder_name and folder_name not in label_dirs:
                label_dirs.append(folder_name)
    log(f'Cartelle da processare {label_dirs}')
    return label_dirs


def get_labels_to_process(id_albums):
    album_details_requests = []
    labels_to_process = []
    if id_albums:
        for (id_rpc, id_album) in enumerate(id_albums, 1):
            json_album_detail_payload = {"jsonrpc": "2.0", "method": "AudioLibrary.GetAlbumDetails", "id": id_rpc,
                                         "params": {"albumid": id_album, "properties": ["albumlabel"]}}
            if json_album_detail_payload not in album_details_requests:
                album_details_requests.append(json_album_detail_payload)
        json_result = xbmc.executeJSONRPC(json.dumps(album_details_requests))
        json_result = json.loads(json_result)
        for single_result in json_result:
            result = single_result.get('result')
            if result:
                album_details = result.get('albumdetails')
                if album_details:
                    record_label = album_details.get('albumlabel')
                    if record_label and record_label not in labels_to_process:
                        labels_to_process.append(record_label)
    return sorted(labels_to_process)


def cleanup_textures(textures, labels_to_remove):
    textures_to_remove = []
    label_logos = build_label_images_dict(get_textures_urls(textures), True)
    for label_special_path in labels_to_remove:
        texturized_path = label_logos.get(label_special_path)
        for texture in textures:
            texture_url = texture.get('url')
            texture_id = texture.get('textureid')
            if texture_url == texturized_path:
                log(f'Rimuovo la texture {texture_id} associata a {label_special_path}')
                textures_to_remove.append(texture)
                break

    if textures_to_remove:
        id_textures = [texture.get('textureid') for texture in textures_to_remove]
        remove_textures(id_textures)


def build_label_images_dict(textures, bypass_texture_filter):
    label_logos = {}
    dirs = xbmcvfs.listdir(os.path.join(etichette_local_special))[0]
    for directory in dirs:
        dir_path = f'{etichette_local_special + directory}/'
        files = xbmcvfs.listdir(dir_path)[1]
        for file in files:
            if file.endswith('.jpg') or file.endswith('.png'):
                file_path = os.path.join(dir_path, file)
                result = get_kodi_image_path(file_path)
                if bypass_texture_filter or result not in textures:
                    label_logos[file_path] = result
    return label_logos


def preload_new_labels_on_texture_cache(textures_results):
    textures = get_textures_urls(textures_results)
    label_logos = build_label_images_dict(textures, False)

    progress = xbmcgui.DialogProgressBG()
    progress.create(addon_name, "Preloading Labels Textures")
    try:
        total_label_to_process = len(label_logos.keys())
        for (step, label_logo) in enumerate(label_logos.keys(), 1):
            with xbmcvfs.File(label_logos.get(label_logo)):
                percentuale = (step / total_label_to_process) * 100

            splitted_path = label_logo.split('/')
            file_name = splitted_path[len(splitted_path) - 1]
            progress.update(percent=int(percentuale), message=file_name)
    finally:
        progress.close()


def get_kodi_image_path(file_path):
    # decodifico il file path con il path image per Kodi per triggerare il job di cache
    # Kodi goes lowercase and doesn't encode some chars
    texture_url = 'image://{0}/'.format(quote(file_path, '()!'))
    texture_url = re.sub(r'%[0-9A-F]{2}', lambda mo: mo.group().lower(), texture_url)
    # Sostituisci manualmente il carattere `~` con la sua codifica
    texture_url = texture_url.replace('~', '%7e')
    return texture_url


def get_label_paths_from_location(location):
    labels = {}
    dirs = xbmcvfs.listdir(os.path.join(location))[0]
    root_files = xbmcvfs.listdir(os.path.join(location))[1]
    if root_files:
        labels[location] = root_files
    for directory in dirs:
        dir_path = f'{location + directory}{os.sep}'
        files = xbmcvfs.listdir(dir_path)[1]
        if files:
            filtered_files = [file for file in files]
            labels[directory] = filtered_files
    return labels


def remove_labels(labels, textures, use_webdav):
    labels_removed = False
    local_labels = get_label_paths_from_location(etichette_local)
    label_names_central = tuple()
    for label in labels:
        label_names_central += (f'{get_label_filename(label)}',)
    label_names_local = get_label_names(local_labels, use_webdav)
    labels_to_remove = [label for label in label_names_local if
                        not label.startswith(label_names_central) and label != 'index.xml']
    # qui raccolgo le label da ripristinare l'icon di default
    labels_to_update = []
    texture_to_cleanup = []
    paths_to_remove = []
    for label_to_remove in labels_to_remove:
        for label_folder in local_labels.keys():
            if label_to_remove in local_labels.get(label_folder):
                path_to_remove = os.path.join(etichette_local, label_folder, label_to_remove)
                paths_to_remove.append(path_to_remove)
                if not label_to_remove.endswith('.xml'):
                    special_path = f'{etichette_local_special}{label_folder}/{label_to_remove}'
                    texture_to_cleanup.append(special_path)
                    file_stem = label_to_remove.split('.')[0]
                    label_node_file_name = f'{file_stem}.xml'
                    if label_node_file_name not in labels_to_remove:
                        labels_to_update.append(f'{etichette_local_special}{label_folder}/{label_node_file_name}')
                break

    log(f'Label da rimuovere {paths_to_remove}')
    log(f'Texture da rimuovere {texture_to_cleanup}')
    log(f'Label da aggiornare con l\'icona di default {labels_to_update}')

    # Aggiorno con l'icona di default
    if labels_to_update:
        for label in labels_to_update:
            set_default_icon(label)

    # la pulizia delle texture va effettuata prima della cancellazione effettiva sennò perdo i riferimenti
    cleanup_textures(textures, texture_to_cleanup)

    progress = xbmcgui.DialogProgressBG()
    progress.create(addon_name, "Rimuovo le case discografiche")
    try:
        total_label_to_process = len(paths_to_remove)

        for (step, path_to_remove) in enumerate(paths_to_remove, 1):
            msg = f'Rimuovo {path_to_remove}'
            log(msg)
            xbmcvfs.delete(path_to_remove)
            percentuale = (step / total_label_to_process) * 100
            progress.update(percent=int(percentuale), message=msg)
            labels_removed = True
    finally:
        progress.close()
    return labels_removed


def get_label_names(labels_dict, use_webdav):
    label_names = []
    for alpha_label in labels_dict.keys():
        for file in labels_dict.get(alpha_label):
            file_name = get_label_name(file)
            if file_name not in label_names:
                label_names.append(db_scan.encode_string(file_name) if use_webdav else file_name)
    return label_names


def get_label_name(file_path):
    splitted_path = file_path.split(os.sep)
    file_name = splitted_path[len(splitted_path) - 1]
    return file_name


def get_id_albums_from_paths(id_albums, scanned_path):
    get_directory_payload = {
        "jsonrpc": "2.0",
        "method": "Files.GetDirectory",
        "id": "1",
        "params": {
            "directory": scanned_path,
            "media": "music",
            "properties": [
                "albumid"
            ]
        }
    }
    json_result = json.loads(xbmc.executeJSONRPC(json.dumps(get_directory_payload, ensure_ascii=False))).get('result')
    if json_result:
        for file in json_result.get('files'):
            if file.get('albumid') and file.get('albumid') not in id_albums:
                id_albums.append(file.get('albumid'))
            elif file.get('type') == 'unknown' and file.get('filetype') == 'directory':
                get_id_albums_from_paths(id_albums, file.get('file'))


def get_ids_to_refresh(paths, use_webdav):
    scanned_paths = []
    id_albums = []
    for path in paths:
        path = db_scan.convert_from_smb_to_davs(path) if use_webdav else path
        if path not in scanned_paths:
            scanned_paths.append(path)
    for scanned_path in scanned_paths:
        get_id_albums_from_paths(id_albums, scanned_path)
    return id_albums


def get_labels():
    json_payload = {
        "jsonrpc": "2.0",
        "method": "AudioLibrary.GetAlbums",
        "id": "1",
        "params": {
            "properties": [
                "albumlabel"
            ]
        }
    }
    response = xbmc.executeJSONRPC(json.dumps(json_payload))
    json_response = json.loads(response)
    albums_label = []
    albums = json_response.get('result').get('albums')
    for album in albums:
        if album.get('albumlabel') not in albums_label:
            albums_label.append(album.get('albumlabel'))
    return sorted(albums_label)


def get_labels_by_initial_letter(labels):
    return build_labels_by_initial_letter(labels)


def build_labels_by_initial_letter(labels):
    labels_by_initial_letter = {}
    for label in labels:
        if label:
            initial = label[0]
            not_alphanumeric = initial in string.punctuation or initial in string.digits or unicodedata.category(
                initial) == 'Lo'
            key = non_alphabetical_folder if not_alphanumeric else unidecode(initial).upper()
            labels_list = labels_by_initial_letter.get(key)
            if not labels_list:
                labels_list = []
            if label not in labels_list:
                labels_list.append(label)
            labels_by_initial_letter[key] = labels_list

    return labels_by_initial_letter


def get_labels_to_process_by_initial_letter(labels_to_process):
    return build_labels_by_initial_letter(labels_to_process)


# genero il numero di ordinamento sequenziale guardando le label presenti nel db
def get_label_order_number(label, labels_by_initial_letter, label_folder_name):
    labels = labels_by_initial_letter.get(label_folder_name)
    if label not in labels:
        labels.append(label)
    labels_sorted = sorted(labels)

    order_number_by_label_name = {}
    for (order_index, label_name) in enumerate(labels_sorted, start=1):
        order_number_by_label_name[label_name] = order_index

    return order_number_by_label_name.get(label)


def update_label_order(label_to_update, labels_by_initial_letter):
    splitted_path = label_to_update.split('\\')
    label_file_name = splitted_path[len(splitted_path) - 1]
    translated_path = xbmcvfs.translatePath(label_to_update)
    label_etree = ET.parse(translated_path)
    label_root = label_etree.getroot()
    label_value = label_root.find('rule').find('value').text
    label_folder_name = get_label_folder_name(label_value)
    order = get_label_order_number(label_value, labels_by_initial_letter, label_folder_name)
    log(f'Aggiungo ordine {order} alla label {label_value} (file {label_file_name})')
    label_root.set('order', str(order))
    label_etree.write(translated_path, encoding='UTF-8')


def set_icon(label_to_update, icon_to_set):
    splitted_path = label_to_update.split('\\')
    label_file_name = splitted_path[len(splitted_path) - 1]
    label_etree = ET.parse(label_to_update)
    label_root = label_etree.getroot()
    label_value = label_root.find('rule').find('value').text
    label_icon_tag = label_root.find('icon')
    label_icon_value = label_icon_tag.text
    log(f'Attuale valore icon {label_icon_value}')
    if label_icon_value != icon_to_set:
        log(f'Imposto icona alla label {label_value} (file {label_file_name})')
        label_icon_tag.text = icon_to_set
        label_etree.write(label_to_update, encoding='UTF-8')


def set_default_icon(label_to_update):
    translated_path = xbmcvfs.translatePath(label_to_update)
    set_icon(translated_path, kodi_label_default_icon)


def get_potential_filenames_from_labels_to_process(labels_to_process):
    potential_filenames = []
    if labels_to_process:
        for label in labels_to_process:
            label_filename = get_label_filename(label)
            if label_filename not in potential_filenames:
                potential_filenames.append(label_filename)
    return potential_filenames


def update_labels(central_labels_dir, textures, albums_to_check, labels_to_check, use_webdav,
                  labels_by_initial_letter):
    # uso le chiavi di central labels perchè sono comuni anche con local labels
    # (sarebbero le cartelle per ordine alfabetico)
    progress = xbmcgui.DialogProgressBG()
    progress.create(addon_name, "Controllo le case discografiche da aggiornare")
    try:
        labels_to_update = {}
        textures_to_refresh = []
        special_texture_by_label = {}
        log(f'Label da controllare {labels_to_check}')
        label_dirs = get_labels_dirs(albums_to_check, labels_to_check)
        potential_filenames = get_potential_filenames_from_labels_to_process(labels_to_check)
        labels_to_process_by_initial_letter = get_labels_to_process_by_initial_letter(labels_to_check)
        sorted_central_labels_dir = []
        if label_dirs:
            sorted_central_labels_dir = sorted(label_dirs)

        for directory in sorted(labels_to_process_by_initial_letter.keys()):
            central_directory = db_scan.encode_string(directory) if use_webdav else directory
            central_files = central_labels_dir.get(central_directory)
            total_label_to_process = len(central_files)

            for (step, file) in enumerate(central_files, 1):
                central_file_path = f'{central_etichette_path + directory}/{file}'
                directory = unquote(directory) if use_webdav else directory
                file = unquote(file) if use_webdav else file
                local_file_path = os.path.join(etichette_local, directory, file)
                if not file.endswith('.xml'):
                    for potential_filename in potential_filenames:
                        if potential_filename in file:
                            labels_to_update[central_file_path] = local_file_path
                            special_path = f'{etichette_local_special}{directory}/{file}'
                            file_stem = file.split('.')[0]
                            label_node_file_name = f'{file_stem}.xml'
                            special_texture_by_label[label_node_file_name] = special_path
                            textures_to_refresh.append(special_path)
                            break
                    percentuale = (step / total_label_to_process) * 100
                    progress.update(percent=int(percentuale), message=f'Label da aggiornare {file}')

        for directory in sorted_central_labels_dir:
            central_directory = db_scan.encode_string(directory) if use_webdav else directory
            central_files = central_labels_dir.get(central_directory)
            total_label_to_process = len(central_files)
            for (step, file) in enumerate(central_files, 1):
                directory = unquote(directory) if use_webdav else directory
                file = unquote(file) if use_webdav else file
                local_file_path = os.path.join(etichette_local, directory, file)
                if file != 'index.xml' and file.endswith('.xml'):
                    update_label_order(local_file_path, labels_by_initial_letter)
                    special_icon = special_texture_by_label.get(file)
                    if special_icon:
                        set_icon(local_file_path, special_icon)
                    percentuale = (step / total_label_to_process) * 100
                    progress.update(percent=int(percentuale), message=f'Aggiorno {file}')

    finally:
        progress.close()

    if labels_to_update:
        progress = xbmcgui.DialogProgressBG()
        progress.create(addon_name, "Updating Labels")
        try:
            total_label_to_process = len(labels_to_update.keys())
            for (step, central_path) in enumerate(labels_to_update.keys(), 1):
                local_file_path = labels_to_update.get(central_path)
                removed_label = xbmcvfs.delete(local_file_path)
                copied_label = xbmcvfs.copy(central_path, local_file_path)
                updated_label = removed_label and copied_label
                if updated_label:
                    log(f'Aggiornato {local_file_path}')
                    percentuale = (step / total_label_to_process) * 100
                    progress.update(percent=int(percentuale), message=f'Aggiornato {get_label_name(local_file_path)}')
        finally:
            progress.close()

        if textures_to_refresh:
            cleanup_textures(textures, textures_to_refresh)


def force_confluence_wall_view(path):
    view_mode_db_path = db_scan.get_view_modes_db_path()
    view_mode_db = sqlite3.connect(view_mode_db_path)
    view_mode_db.set_trace_callback(log)
    view_mode_db_cursor = view_mode_db.cursor()
    query_check = "SELECT * FROM VIEW vista WHERE vista.path = {}"
    query_check = query_check.format(f'\'{path}\'')
    check_res = view_mode_db_cursor.execute(query_check).fetchall()
    if not check_res:
        # inserisco il record sul db delle view mode
        insert_query = "INSERT INTO view (window, path, viewMode, sortMethod, sortOrder, sortAttributes, skin) VALUES (?,?,?,?,?,?,?)"
        insert_values = (10502, path, 66036, 1, 1, 0, 'skin.confluence.zabarchives',)
        view_mode_db_cursor.execute(insert_query, insert_values)
        view_mode_db.commit()
    view_mode_db_cursor.close()
    view_mode_db.close()


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


def init_letter_folder_node(alphabetic_folder, default_icon, order):
    file_name = 'index.xml'
    alphabetic_folder_path = xbmcvfs.translatePath(f'{etichette_local_special}{alphabetic_folder}')
    destination_path = os.path.join(alphabetic_folder_path, file_name)
    node = ET.Element('node', order=str(order))
    label = ET.SubElement(node, 'label')
    label.text = alphabetic_folder if alphabetic_folder != non_alphabetical_folder else '#0'
    icon = ET.SubElement(node, 'icon')
    icon.text = default_icon
    index_xml = minidom.parseString(ET.tostring(node, encoding='UTF-8')).toprettyxml()
    with xbmcvfs.File(destination_path, 'w') as genre_file:
        genre_file.write(index_xml)


def generate_label_node(record_label, order, destination, label_folder_name, label_logo):
    label_filename = get_label_filename(record_label)
    file_name = f'{label_filename}.xml'
    destination_path = os.path.join(destination, file_name)
    node = ET.Element('node', order=str(order), type='filter')
    label = ET.SubElement(node, 'label')
    label.text = re.sub(r'\s*\[\d+]', '', record_label)
    content_tag = ET.SubElement(node, "content")
    content_tag.text = 'albums'
    group_tag = ET.SubElement(node, 'group')
    group_tag.text = 'years'
    rule_tag = ET.SubElement(node, "rule")
    rule_tag.set('field', 'label')
    rule_tag.set('operator', 'is')
    value_tag = ET.SubElement(rule_tag, "value")
    value_tag.text = record_label
    icon_tag = ET.SubElement(node, "icon")
    logo_file_name_path_special = f'special://masterprofile/library/music/etichette/{label_folder_name}/{label_logo}'
    icon_tag.text = 'DefaultMusicAlbums.png' if not label_logo else logo_file_name_path_special
    node_xml = minidom.parseString(ET.tostring(node, encoding='UTF-8')).toprettyxml()
    with xbmcvfs.File(destination_path, 'w') as genre_file:
        genre_file.write(node_xml)


def force_confluence_wall_view_for_labels(label):
    xsp_rule = {
        "group": {
            "mixed": False,
            "type": "years"
        },
        "rules": {
            "and": [
                {
                    "field": "label",
                    "operator": "is",
                    "value": [
                        label
                    ]
                }
            ]
        },
        "type": "albums"
    }
    json_xsp_rule = json.dumps(xsp_rule, separators=(',', ':'), ensure_ascii=False)
    encoded_xsp = db_scan.encode_string(json_xsp_rule, safe_chars=smb_safe_chars)
    path = f'musicdb://years/?xsp={encoded_xsp}'
    force_confluence_wall_view(path)


def preload_labels_on_local_kodi():
    labels_to_transfer = {}
    use_webdav = db_params.get('sourcetype') == 'webdav'
    central_labels = get_label_paths_from_location(central_etichette_path)
    textures = get_textures()
    labels = get_labels()
    labels_by_initial_letter = get_labels_by_initial_letter(labels)
    # inizializzo etichette e il suo relativo nodo
    if not xbmcvfs.exists(etichette_local):
        xbmcvfs.mkdir(etichette_local)
    local_index_node_path = os.path.join(etichette_local, 'index.xml')
    if not xbmcvfs.exists(local_index_node_path):
        init_node(etichette_local, main_node_icon, main_node_label, main_node_order)
    sorted_central_labels = sorted(central_labels)

    # scarica tutte le label disponibili dal percorso centrale
    for (order, directory) in enumerate(sorted_central_labels, 2):
        local_path_node = f'{etichette_local_special}{directory}/'
        if not xbmcvfs.exists(xbmcvfs.translatePath(local_path_node)):
            xbmcvfs.mkdir(local_path_node)
            order_to_set = 1 if directory == non_alphabetical_folder else order
            init_letter_folder_node(directory, folder_node_icon, order_to_set)
        files = central_labels.get(directory)
        if files:
            for file in files:
                source_file_path = f'{central_etichette_path + directory}/{file}'
                if source_file_path not in labels_to_transfer:
                    key = os.path.join(directory, file)
                    labels_to_transfer[key] = source_file_path

    progress = xbmcgui.DialogProgressBG()
    progress.create(addon_name, "Sto caricando i loghi centralizzati delle case discografiche")
    try:
        total_label_to_process = len(labels_to_transfer.keys())
        for (step, label) in enumerate(labels_to_transfer.keys(), 1):
            # in caso di libreria webDAV controllo che il file decodificato sia già presente
            local_label = unquote(label) if use_webdav else label
            destination_path = os.path.join(etichette_local, local_label)
            if not xbmcvfs.exists(destination_path) and destination_path.endswith(allowed_files):
                if xbmcvfs.copy(labels_to_transfer.get(label), destination_path):
                    log(f'Copiato da {labels_to_transfer.get(label)} a {destination_path}')
                    percentuale = (step / total_label_to_process) * 100
                    progress.update(percent=int(percentuale), message=label)
    finally:
        progress.close()

    # genero gli xml delle etichette
    local_labels = get_label_paths_from_location(etichette_local)
    labels_to_generate = []
    for label in labels:
        folder_name = get_label_folder_name(label)
        label_file_name = get_label_filename(label)
        check_path = os.path.join(etichette_local, folder_name, label_file_name)
        if not xbmcvfs.exists(check_path) and label not in labels_to_generate:
            labels_to_generate.append(label)
    progress = xbmcgui.DialogProgressBG()
    progress.create(addon_name, "Sto caricando i loghi centralizzati delle case discografiche")
    try:
        for (order, label) in enumerate(labels_to_generate, 1):
            folder_name = get_label_folder_name(label)
            label_file_name = get_label_filename(label)
            check_path = os.path.join(etichette_local, folder_name, label_file_name)
            if not xbmcvfs.exists(check_path):
                label_logo = None
                partial_filename_match_pattern = rf'^{re.escape(label_file_name)}(?![a-zA-Z])\d*\.'
                filtered_content = list(filter(
                    lambda label_node: re.match(partial_filename_match_pattern, label_node) and not label_node.endswith(
                        '.xml'),
                    local_labels.get(folder_name)))
                if len(filtered_content) == 1:
                    label_logo = filtered_content[0]
                destination_path = os.path.join(etichette_local, folder_name)
                generate_label_node(label, order, destination_path, folder_name, label_logo)
                percentuale = (order / len(labels_to_generate)) * 100
                progress.update(percent=int(percentuale), message=re.sub(r'\s*\[\d+]', '', label))
    finally:
        progress.close()

    remove_labels(labels, textures, use_webdav)

    paths_from_params = db_scan.get_paths_from_params()
    if paths_from_params:
        albums_to_check = get_ids_to_refresh(paths_from_params, use_webdav)
        labels_to_check = get_labels_to_process(albums_to_check)
        local_labels = get_label_paths_from_location(etichette_local)
        for local_label_directory in local_labels:
            if local_label_directory != etichette_local:
                path_to_force = f'library://music/etichette/{local_label_directory}/'
                force_confluence_wall_view(path_to_force)
        update_labels(central_labels, textures, albums_to_check, labels_to_check, use_webdav, labels_by_initial_letter)
        if labels_to_check:
            progress.create(addon_name, "Aggiorno la visualizzazione delle label")
            try:
                total_label_to_process = len(labels_to_check)
                for (step, label) in enumerate(labels_to_check, 1):
                    force_confluence_wall_view_for_labels(label)
                    percentuale = (step / total_label_to_process) * 100
                    progress.update(percent=int(percentuale), message=label)
            finally:
                progress.close()
    preload_new_labels_on_texture_cache(textures)
    builtin_cmd = f'NotifyAll({addon_id}, OnLabelsPreloaded)'
    xbmc.executebuiltin(builtin_cmd)


if __name__ == '__main__':
    addon_name = xbmcaddon.Addon().getAddonInfo('name')
    addon_id = xbmcaddon.Addon().getAddonInfo('id')
    log(addon_name)
    preload_labels_on_local_kodi()
