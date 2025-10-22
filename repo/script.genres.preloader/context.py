import json
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
    full_screen_payload = {"jsonrpc": "2.0", "method": "GUI.SetFullscreen", "id": "1", "params": {"fullscreen": True}}
    xbmc.executeJSONRPC(json.dumps(full_screen_payload))


if __name__ == '__main__':
    execute_party_mode_from_playlist()
