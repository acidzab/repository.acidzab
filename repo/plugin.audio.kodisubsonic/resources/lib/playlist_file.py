"""
Lettura e scrittura dei file playlist M3U/M3U8. Nessuna dipendenza da Kodi: l'accesso ai file passa da un
oggetto `Storage` (in Kodi basato su xbmcvfs, nei test sul filesystem locale).
Scritto per Kodi 22 (Python 3.14).
"""
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import IntEnum
from pathlib import Path
from typing import Protocol

EDITABLE_EXTENSIONS = (".m3u8", ".m3u")
SMART_EXTENSION = ".xsp"
DEFAULT_EXTENSION = ".m3u8"
MAX_NAME_LENGTH = 100

_INVALID_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}


class ErrorCode(IntEnum):
    """Codici di errore OpenSubsonic restituiti al gateway."""
    GENERIC = 0
    NOT_AUTHORIZED = 50
    NOT_FOUND = 70


class PlaylistError(Exception):
    def __init__(self, message: str, code: ErrorCode = ErrorCode.GENERIC):
        super().__init__(message)
        self.code = code


class Storage(Protocol):
    """Accesso ai file della cartella delle playlist, per nome di file."""

    def names(self) -> list[str]: ...

    def exists(self, name: str) -> bool: ...

    def read_text(self, name: str) -> str: ...

    def write_bytes(self, name: str, data: bytes) -> None: ...

    def rename(self, source: str, target: str) -> None: ...

    def delete(self, name: str) -> None: ...


class LocalStorage:
    """Storage su una cartella locale (test e strumenti fuori da Kodi)."""

    def __init__(self, folder: str | Path):
        self.folder = Path(folder)

    def names(self) -> list[str]:
        return [path.name for path in self.folder.iterdir()]

    def exists(self, name: str) -> bool:
        return (self.folder / name).is_file()

    def read_text(self, name: str) -> str:
        return (self.folder / name).read_text(encoding="utf-8")

    def write_bytes(self, name: str, data: bytes) -> None:
        (self.folder / name).write_bytes(data)

    def rename(self, source: str, target: str) -> None:
        (self.folder / source).rename(self.folder / target)

    def delete(self, name: str) -> None:
        (self.folder / name).unlink()


@dataclass(slots=True)
class Entry:
    path: str
    # Righe di commento/direttive (#EXTINF, ...) che precedono la voce
    directives: list[str] = field(default_factory=list)


@dataclass(slots=True)
class Playlist:
    header: list[str] = field(default_factory=lambda: ["#EXTM3U"])
    entries: list[Entry] = field(default_factory=list)
    trailer: list[str] = field(default_factory=list)


def song_entry(path: str, duration: int | None, artist: str | None, title: str | None) -> Entry:
    label = " - ".join(part for part in (artist, title) if part)
    label = label.replace("\r", " ").replace("\n", " ")
    return Entry(path=path, directives=[f"#EXTINF:{int(duration or -1)},{label}"])


# --- Nomi dei file ---

def extension_of(file_name: str) -> str:
    return Path(file_name).suffix.lower()


def is_playlist(file_name: str) -> bool:
    return not file_name.startswith(".") and extension_of(file_name) in (*EDITABLE_EXTENSIONS, SMART_EXTENSION)


def sanitize_name(name: str | None) -> str:
    """Nome della playlist -> nome di file sicuro (senza estensione)."""
    cleaned = _INVALID_CHARS.sub("_", (name or "").strip())
    cleaned = cleaned.strip(" .")[:MAX_NAME_LENGTH].strip(" .")
    if not cleaned:
        raise PlaylistError("Nome della playlist mancante o non valido")
    if cleaned.upper().split(".")[0] in _RESERVED_NAMES:
        cleaned = f"_{cleaned}"
    return cleaned


def validate_file_name(file_name: str | None, extensions: Iterable[str] = EDITABLE_EXTENSIONS) -> str:
    """Accetta solo un nome di file semplice (niente percorsi) con un'estensione ammessa."""
    if (not file_name or "/" in file_name or "\\" in file_name or file_name != Path(file_name).name
            or file_name.startswith(".") or extension_of(file_name) not in tuple(extensions)):
        raise PlaylistError(f"Playlist non valida: {file_name}", ErrorCode.NOT_FOUND)
    return file_name


def unique_file_name(storage: Storage, name: str, extension: str = DEFAULT_EXTENSION,
                     current: str | None = None) -> str:
    """Primo nome libero tra 'name.ext', 'name (2).ext', ... Il file `current` conta come libero (rinomina)."""
    existing = {entry.lower() for entry in storage.names()}
    if current:
        existing.discard(current.lower())
    candidate, counter = f"{name}{extension}", 2
    while candidate.lower() in existing:
        candidate, counter = f"{name} ({counter}){extension}", counter + 1
    return candidate


def renamed_file_name(storage: Storage, current: str, name: str | None) -> str:
    if not name:
        return current
    base = sanitize_name(name)
    if f"{base}{extension_of(current)}" == current:
        return current
    return unique_file_name(storage, base, extension_of(current), current)


# --- Parsing ---

def parse(text: str) -> Playlist:
    playlist = Playlist(header=[])
    pending: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            # Le direttive prima della prima voce sono globali, tranne #EXTINF che appartiene alla voce
            if not playlist.entries and not pending and not line.upper().startswith("#EXTINF"):
                playlist.header.append(line)
            else:
                pending.append(line)
            continue
        playlist.entries.append(Entry(path=line, directives=pending))
        pending = []
    playlist.trailer = pending
    if not playlist.header or playlist.header[0].upper() != "#EXTM3U":
        playlist.header.insert(0, "#EXTM3U")
    return playlist


def dump(playlist: Playlist) -> str:
    lines = list(playlist.header)
    for entry in playlist.entries:
        lines.extend(entry.directives)
        lines.append(entry.path)
    lines.extend(playlist.trailer)
    return "\n".join(lines) + "\n"


# --- Modifiche ---

def remove_indices(playlist: Playlist, indices: Iterable[int]) -> None:
    unique = sorted(set(indices), reverse=True)
    if invalid := [index for index in unique if not 0 <= index < len(playlist.entries)]:
        raise PlaylistError(f"Indice non valido: {invalid[-1]}")
    for index in unique:
        del playlist.entries[index]


# --- File ---

def _require(storage: Storage, file_name: str | None) -> str:
    file_name = validate_file_name(file_name)
    if not storage.exists(file_name):
        raise PlaylistError(f"Playlist non trovata: {file_name}", ErrorCode.NOT_FOUND)
    return file_name


def read(storage: Storage, file_name: str | None) -> Playlist:
    """Playlist in UTF-8 (il BOM iniziale, se presente, si ignora)."""
    return parse(storage.read_text(_require(storage, file_name)).removeprefix("﻿"))


def write(storage: Storage, file_name: str, playlist: Playlist) -> None:
    """
    Scrittura sicura anche su condivisioni di rete, dove rinominare sopra un file esistente non è garantito:
    file temporaneo, il vecchio file diventa un backup, il temporaneo prende il suo posto, il backup si elimina.
    """
    file_name = validate_file_name(file_name)
    temp, backup = f".{file_name}.tmp", f".{file_name}.bak"
    storage.write_bytes(temp, dump(playlist).encode("utf-8"))
    if not storage.exists(file_name):
        storage.rename(temp, file_name)
        return
    if storage.exists(backup):
        storage.delete(backup)
    storage.rename(file_name, backup)
    try:
        storage.rename(temp, file_name)
    except Exception:
        storage.rename(backup, file_name)
        raise
    storage.delete(backup)


def save(storage: Storage, file_name: str, playlist: Playlist, current: str | None = None) -> None:
    """Scrive la playlist in `file_name`; se diverso da `current` (rinomina) elimina il vecchio file."""
    if current and current != file_name and current.lower() == file_name.lower():
        # Cambiano solo maiuscole/minuscole: su filesystem case-insensitive è lo stesso file
        storage.rename(_require(storage, current), validate_file_name(file_name))
        current = file_name
    write(storage, file_name, playlist)
    if current and current != file_name:
        delete(storage, current)


def delete(storage: Storage, file_name: str | None) -> None:
    storage.delete(_require(storage, file_name))


def list_names(storage: Storage) -> list[str]:
    return sorted((name for name in storage.names() if is_playlist(name)), key=str.lower)
