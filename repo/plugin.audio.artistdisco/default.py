import sys
import json
from urllib.parse import urlencode, parse_qsl, quote

import xbmc
import xbmcgui
import xbmcplugin

HANDLE = int(sys.argv[1])
BASE = sys.argv[0]

ALBUM_ARTISTS_ONLY = True   # False: include anche artisti presenti solo come song artist
HIDE_EMPTY = True
LETTERS = [chr(c) for c in range(ord("A"), ord("Z") + 1)]
PROPS = ["thumbnail", "fanart", "description", "genre", "disambiguation"]
ICON_FOLDER = {"icon": "DefaultFolder.png", "thumb": "DefaultFolder.png"}
ICON_ARTIST = "DefaultArtist.png"
ALPHANUMERIC_FOLDER = "#0"

FORCE_VIEW = True
# skin -> {tipo contenuto: id del controllo view}; gli id vanno verificati per ogni skin/versione
VIEWS = {
    "skin.estuary.zabarchives": {"artists": 54},
    "skin.confluence.zabarchives": {"artists": 512, "files": 500},
}
SORT = {"method": "artist", "ignorearticle": True}


def _is(field, op, value):
    return {"field": field, "operator": op, "value": [value]}


# (chiave, etichetta, builder regole)
CATEGORIES = [
    ("album", "Album", lambda n: [
        _is("albumartist", "is", n), _is("type", "contains", "album"),
        _is("type", "doesnotcontain", "compilation"),
        _is("type", "doesnotcontain", "dj-mix")]),
    ("ep", "EP", lambda n: [_is("albumartist", "is", n), _is("type", "contains", "ep")]),
    ("single", "Singoli", lambda n: [_is("albumartist", "is", n), _is("type", "contains", "single")]),
    ("compilation", "Compilation", lambda n: [_is("albumartist", "is", n), _is("type", "contains", "compilation")]),
    ("djmix", "DJ Mix", lambda n: [_is("albumartist", "is", n), _is("type", "contains", "dj-mix")]),
    ("appears", "Appare su", lambda n: [_is("artist", "is", n), _is("albumartist", "isnot", n)]),
]


def rpc(method, params):
    req = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    return json.loads(xbmc.executeJSONRPC(json.dumps(req))).get("result", {})


def url(**kw):
    return BASE + "?" + urlencode(kw)


def musicdb_path(rules, artist_id):
    xsp = {"type": "albums", "rules": {"and": rules}}
    return "musicdb://albums/?artistid=%d&xsp=%s" % (
        int(artist_id), quote(json.dumps(xsp, ensure_ascii=False), safe=""))


def has_items(path):
    # verifica sul path reale che verra' aperto (testa anche l'option artistid)
    res = rpc("Files.GetDirectory", {"directory": path, "media": "music",
                                     "limits": {"start": 0, "end": 1}})
    return res.get("limits", {}).get("total", 1) > 0


def bucket(name):
    c = name[:1].upper()
    return c if c in LETTERS else ALPHANUMERIC_FOLDER


def all_artists_light():
    return rpc("AudioLibrary.GetArtists", {
        "albumartistsonly": ALBUM_ARTISTS_ONLY, "sort": SORT}).get("artists", [])


def set_view(kind):
    if not FORCE_VIEW:
        return
    vid = VIEWS.get(xbmc.getSkinDir(), {}).get(kind)
    if not vid:
        return
    # endOfDirectory ritorna prima che il container mostri questo path: aspetto che sia pronto
    target = BASE + sys.argv[2]
    mon = xbmc.Monitor()
    for _ in range(30):  # max ~3 s
        if xbmc.getInfoLabel("Container.FolderPath") == target:
            break
        if mon.waitForAbort(0.1):
            return
    else:
        return  # il container non e' mai arrivato su questo path: non toccare la view di altri path
    if xbmc.getCondVisibility("Control.IsVisible(%d)" % vid):
        return  # e' gia' la view corrente: evita un SetViewMode inutile (e un salvataggio nel DB)
    xbmc.executebuiltin("Container.SetViewMode(%d)" % vid)


def _folder_item(label, fanart=""):
    li = xbmcgui.ListItem(label)
    art = dict(ICON_FOLDER)
    if fanart:
        art["fanart"] = fanart
    li.setArt(art)
    return li


def list_letters():
    xbmcplugin.setContent(HANDLE, "files")
    xbmcplugin.setPluginCategory(HANDLE, xbmc.getLocalizedString(133))  # core string "Artists"
    letters = {bucket(a["artist"]) for a in all_artists_light()}
    for l in sorted(letters, key=lambda x: (x != ALPHANUMERIC_FOLDER, x)):
        xbmcplugin.addDirectoryItem(
            HANDLE, url(action="letter", l=l), _folder_item(l), True)
    xbmcplugin.endOfDirectory(HANDLE, cacheToDisc=False)


def add_artist(a):
    name = a.get("artist") or a.get("label", "")
    li = xbmcgui.ListItem(name)
    thumb = a.get("thumbnail") or ICON_ARTIST
    li.setArt({"thumb": thumb, "icon": thumb, "fanart": a.get("fanart", "")})
    li.setLabel2(a.get("disambiguation", ""))
    tag = li.getMusicInfoTag()
    tag.setArtist(name)
    # Estuary (pannello info): bio da Artist_Description se DBType=artist, altrimenti lista release via DBID
    tag.setDbId(a["artistid"], "artist")
    tag.setGenres(a.get("genre", []))
    genres = " / ".join(a.get("genre", []))
    li.setProperty("Artist_Description", a.get("description", ""))
    li.setProperty("Artist_Genre", genres)
    li.setProperty("Artist_Disambiguation", a.get("disambiguation", ""))
    li.setProperty("InfoListPath", "musicdb://artists/%d/" % a["artistid"])  # letta da InfoListPathVar della skin
    xbmcplugin.addDirectoryItem(
        HANDLE, url(action="artist", id=a["artistid"], name=name), li, True)


def list_letter(letter):
    xbmcplugin.setContent(HANDLE, "artists")
    xbmcplugin.setPluginCategory(HANDLE, xbmc.getLocalizedString(133))
    if letter == ALPHANUMERIC_FOLDER:
        for a in all_artists_light():
            if bucket(a["artist"]) == ALPHANUMERIC_FOLDER:
                add_artist(rpc("AudioLibrary.GetArtistDetails", {
                    "artistid": a["artistid"], "properties": PROPS}
                               ).get("artistdetails", a))
    else:
        res = rpc("AudioLibrary.GetArtists", {
            "albumartistsonly": ALBUM_ARTISTS_ONLY, "properties": PROPS, "sort": SORT,
            "filter": {"field": "artist", "operator": "startswith", "value": letter}})
        for a in res.get("artists", []):
            if bucket(a["artist"]) == letter:  # il LIKE di MariaDB e' accent-insensitive
                add_artist(a)
    xbmcplugin.endOfDirectory(HANDLE, cacheToDisc=False)
    set_view("artists")


def list_artist(artist_id, name):
    xbmcplugin.setContent(HANDLE, "files")
    xbmcplugin.setPluginCategory(HANDLE, name)
    fanart = rpc("AudioLibrary.GetArtistDetails", {
        "artistid": int(artist_id), "properties": ["fanart"]
    }).get("artistdetails", {}).get("fanart", "")

    for _key, label, build in CATEGORIES:
        path = musicdb_path(build(name), artist_id)
        if HIDE_EMPTY and not has_items(path):
            continue
        xbmcplugin.addDirectoryItem(HANDLE, path, _folder_item(label, fanart), True)
    xbmcplugin.endOfDirectory(HANDLE, cacheToDisc=False)
    set_view("files")


def router():
    p = dict(parse_qsl(sys.argv[2][1:]))
    action = p.get("action")
    if action == "letter":
        list_letter(p["l"])
    elif action == "artist":
        list_artist(p["id"], p["name"])
    else:
        list_letters()


router()