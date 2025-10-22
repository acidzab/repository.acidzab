import os
import re
import sys

import xbmc
import xbmcvfs

genres_local_special = 'special://masterprofile/library/music/genres/'
genres_local = xbmcvfs.translatePath(genres_local_special)


def execute_party_mode_from_playlist():
    current_item = sys.listitem
    label = current_item.getLabel()
    file_name = re.sub(r"(\W+)", '', label.lower(), flags=re.MULTILINE) + ".xsp"
    playlist_path = os.path.join(genres_local, file_name)
    xbmc.executebuiltin(f"PlayerControl(Partymode({playlist_path}))")


if __name__ == '__main__':
    execute_party_mode_from_playlist()
