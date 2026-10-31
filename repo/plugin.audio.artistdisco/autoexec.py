import xbmcvfs

from default import ARTISTS_NODE_LOCAL_PATH, generate_artists_folder_node

if __name__ == '__main__':
    if not xbmcvfs.exists(ARTISTS_NODE_LOCAL_PATH):
        generate_artists_folder_node()
