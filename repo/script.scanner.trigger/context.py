import sys

import db_scan

from default import align_media_to_central_db


def align_item_to_central_db():
    db_params = db_scan.get_db_params()
    choosen_item = sys.listitem
    choosen_path = choosen_item.getPath()
    central_path = choosen_path
    use_webdav = db_params.get('sourcetype') == 'webdav'
    if use_webdav:
        central_path = db_scan.convert_from_davs_to_smb(choosen_path)
    align_media_to_central_db([central_path], [choosen_path], 'align', db_params)


if __name__ == '__main__':
    align_item_to_central_db()
