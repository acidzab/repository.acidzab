"""
Plugin delle playlist per Kodi OpenSubsonic Gateway (Kodi 22, Python 3.14, xbmc.python 3.1.0).

Il gateway lo invoca con JSON-RPC Files.GetDirectory su
    plugin://plugin.audio.kodisubsonic/?action=<azione>&...&nonce=<casuale>
Kodi attende la fine del plugin prima di rispondere, quindi la chiamata è sincrona. L'esito torna come label
JSON dell'unico elemento della lista: {"ok": true, "result": ...} oppure {"ok": false, "code": n, "error": "..."}.

Azioni:
    create  name, songs[, file]           nuova playlist (o sostituisce i brani di `file`)
    update  file[, name][, add][, remove][, expect]
                                          rinomina, aggiunge brani in coda, rimuove per indice; con `expect`
                                          (voci lette dal gateway) rifiuta la modifica se ne legge un numero diverso
    delete  file                          elimina la playlist
`songs`/`add` sono ID di brani Kodi separati da virgola, `remove` indici (base 0) separati da virgola.
Non serve per la consultazione da interfaccia: aperto da Kodi senza action non mostra niente.

La cartella delle playlist può essere rimappata da Kodi (ad esempio su una condivisione smb://). Elenco, Stat e
scrittura sul percorso special:// seguono la rimappatura, ma xbmcvfs.File in lettura no: apre il percorso locale
(/data/.kodi/userdata/...) e restituisce vuoto. Il plugin ricava quindi il percorso reale della cartella da
Files.GetDirectory sulla cartella special:// e fa tutte le operazioni su quello.
"""
import json
import sys
import traceback
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs

import xbmc
import xbmcgui
import xbmcplugin
import xbmcvfs

from resources.lib import playlist_file
from resources.lib.playlist_file import ErrorCode, PlaylistError

ADDON_ID = "plugin.audio.kodisubsonic"
PLAYLIST_FOLDER = "special://profile/playlists/music/"
SONG_PROPERTIES = ["file", "title", "displayartist", "duration"]

type Params = dict[str, str]


def _json_rpc(method: str, params: dict) -> dict:
    response = json.loads(xbmc.executeJSONRPC(json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                                                           "params": params})))
    return response.get("result") or {}


def _real_folder(folder: str) -> str:
    """
    Percorso reale della cartella (es. smb://...): Files.GetDirectory restituisce i file con il percorso dopo le
    sostituzioni di Kodi. Con la cartella vuota non c'è niente da leggere e si resta sul percorso special://.
    """
    for item in _json_rpc("Files.GetDirectory", {"directory": folder, "media": "files"}).get("files") or []:
        path = (item.get("file") or "").rstrip("/")
        if "/" in path and not path.startswith(folder):
            return path.rsplit("/", 1)[0] + "/"
    return folder


class KodiStorage:
    """Storage sul VFS di Kodi (playlist_file.Storage), sul percorso reale della cartella delle playlist."""

    def __init__(self, folder: str = PLAYLIST_FOLDER):
        if not xbmcvfs.exists(folder):
            xbmcvfs.mkdirs(folder)
        self.folder = _real_folder(folder)
        xbmc.log(f"[{ADDON_ID}] cartella delle playlist: {folder} -> {self.folder}", xbmc.LOGDEBUG)

    def _path(self, name: str) -> str:
        return self.folder + name

    def names(self) -> list[str]:
        # Kodi può classificare i file playlist come cartelle (si possono aprire): servono entrambe le liste
        dirs, files = xbmcvfs.listdir(self.folder)
        return [*dirs, *files]

    def exists(self, name: str) -> bool:
        return xbmcvfs.exists(self._path(name))

    def read_text(self, name: str) -> str:
        """
        Testo UTF-8 della playlist con xbmcvfs.File.read(). Una lettura più corta della dimensione del file
        (xbmcvfs.Stat) è un errore: riscrivere la playlist da una lettura vuota la cancellerebbe.
        """
        path = self._path(name)
        size = xbmcvfs.Stat(path).st_size()
        with xbmcvfs.File(path) as f:
            text = f.read()
        read = len(text.encode("utf-8"))
        xbmc.log(f"[{ADDON_ID}] lettura {path}: {read} byte, dimensione {size}", xbmc.LOGINFO)
        if read < size:
            raise PlaylistError(f"Lettura incompleta della playlist ({read} byte su {size}): nessuna modifica")
        return text

    def write_bytes(self, name: str, data: bytes) -> None:
        with xbmcvfs.File(self._path(name), "w") as f:
            if not f.write(bytearray(data)):
                raise OSError(f"Scrittura non riuscita: {self._path(name)}")

    def rename(self, source: str, target: str) -> None:
        if not xbmcvfs.rename(self._path(source), self._path(target)):
            raise OSError(f"Rinomina non riuscita: {source} -> {target}")

    def delete(self, name: str) -> None:
        if not xbmcvfs.delete(self._path(name)):
            raise OSError(f"Eliminazione non riuscita: {self._path(name)}")


def _int_list(value: str | None, what: str) -> list[int]:
    try:
        return [int(item) for item in (value or "").split(",") if item.strip()]
    except ValueError:
        raise PlaylistError(f"Valore non valido per {what}: {value}") from None


def _song_entries(song_ids: list[int]) -> list[playlist_file.Entry]:
    """Percorsi e metadati dei brani, con un'unica chiamata JSON-RPC in batch."""
    if not song_ids:
        return []
    calls = [{"jsonrpc": "2.0", "id": index, "method": "AudioLibrary.GetSongDetails",
              "params": {"songid": song_id, "properties": SONG_PROPERTIES}}
             for index, song_id in enumerate(song_ids)]
    responses = json.loads(xbmc.executeJSONRPC(json.dumps(calls)))
    if not isinstance(responses, list):  # batch rifiutato: una chiamata per brano
        responses = [json.loads(xbmc.executeJSONRPC(json.dumps(call))) for call in calls]
    by_index = {response.get("id"): response for response in responses}
    entries = []
    for index, song_id in enumerate(song_ids):
        details = (by_index.get(index, {}).get("result") or {}).get("songdetails") or {}
        if not details.get("file"):
            raise PlaylistError(f"Brano non trovato: {song_id}", ErrorCode.NOT_FOUND)
        entries.append(playlist_file.song_entry(details["file"], details.get("duration"),
                                                details.get("displayartist"), details.get("title")))
    return entries


# --- Azioni ---

def action_create(storage: KodiStorage, params: Params) -> str:
    entries = _song_entries(_int_list(params.get("songs"), "songs"))
    if current := params.get("file"):
        playlist = playlist_file.read(storage, current)
        playlist.entries = entries
        target = playlist_file.renamed_file_name(storage, current, params.get("name"))
    else:
        playlist = playlist_file.Playlist(entries=entries)
        target = playlist_file.unique_file_name(storage, playlist_file.sanitize_name(params.get("name")))
    playlist_file.save(storage, target, playlist, current)
    return target


def _check_expected(playlist: playlist_file.Playlist, params: Params) -> None:
    """Il gateway indica quante voci ha letto lui: se il plugin ne legge altre, non si tocca il file."""
    if (expected := params.get("expect")) is None:
        return
    if len(playlist.entries) != int(expected):
        raise PlaylistError(f"La playlist risulta di {len(playlist.entries)} voci invece di {expected} "
                            f"(lettura non riuscita o file modificato nel frattempo): nessuna modifica")


def action_update(storage: KodiStorage, params: Params) -> str:
    current = params.get("file")
    playlist = playlist_file.read(storage, current)
    _check_expected(playlist, params)
    playlist_file.remove_indices(playlist, _int_list(params.get("remove"), "remove"))
    playlist.entries.extend(_song_entries(_int_list(params.get("add"), "add")))
    target = playlist_file.renamed_file_name(storage, current, params.get("name"))
    playlist_file.save(storage, target, playlist, current)
    return target


def action_delete(storage: KodiStorage, params: Params) -> str:
    playlist_file.delete(storage, params.get("file"))
    return params["file"]


ACTIONS: dict[str, Callable[[KodiStorage, Params], Any]] = {
    "create": action_create,
    "update": action_update,
    "delete": action_delete,
}


# --- Kodi ---

def _respond(handle: int, payload: dict) -> None:
    item = xbmcgui.ListItem(label=json.dumps(payload, ensure_ascii=False), offscreen=True)
    xbmcplugin.addDirectoryItem(handle, f"{sys.argv[0]}?result", item, isFolder=False)
    xbmcplugin.endOfDirectory(handle, succeeded=True, cacheToDisc=False)


def _run(params: Params) -> dict:
    action = params.get("action", "")
    try:
        if (handler := ACTIONS.get(action)) is None:
            raise PlaylistError(f"Azione sconosciuta: {action}")
        return {"ok": True, "result": handler(KodiStorage(), params)}
    except PlaylistError as exc:
        return {"ok": False, "code": int(exc.code), "error": str(exc)}
    except Exception as exc:  # l'errore deve arrivare al gateway, non solo al log di Kodi
        xbmc.log(f"[{ADDON_ID}] {traceback.format_exc()}", xbmc.LOGERROR)
        return {"ok": False, "code": int(ErrorCode.GENERIC), "error": f"Errore interno del plugin: {exc}"}


def main() -> None:
    handle = int(sys.argv[1])
    query = sys.argv[2].removeprefix("?") if len(sys.argv) > 2 else ""
    params = {key: values[-1] for key, values in parse_qs(query, keep_blank_values=True).items()}
    if not params.get("action"):  # aperto dall'interfaccia: nessuna cartella da mostrare
        xbmcplugin.endOfDirectory(handle, succeeded=False, cacheToDisc=False)
    else:
        _respond(handle, _run(params))


if __name__ == "__main__":
    main()
