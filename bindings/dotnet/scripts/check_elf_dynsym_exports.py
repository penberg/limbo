#!/usr/bin/env python3

import struct
import sys
from pathlib import Path

NAME = 0
TYPE = 1
OFFSET = 4
SIZE = 5
LINK = 6
ENTRY_SIZE = 9


def main() -> None:
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        run_self_test()
        return
    if len(sys.argv) < 3:
        raise SystemExit("usage: check_elf_dynsym_exports.py <elf> <symbol>...")

    path = Path(sys.argv[1])
    needed = sys.argv[2:]
    missing = missing_exports(path, needed)
    if missing:
        raise SystemExit(f"{path} is missing dynsym exports: {', '.join(missing)}")


def run_self_test() -> None:
    import tempfile

    symbol = "turso_sync_database_new"
    with tempfile.NamedTemporaryFile(delete=False, suffix=".so") as handle:
        handle.write(elf64_with_dynsym(symbol))
        path = Path(handle.name)
    try:
        if missing_exports(path, [symbol]):
            raise SystemExit("self-test failed: expected export was not found")
        if not missing_exports(path, ["turso_sync_database_missing"]):
            raise SystemExit("self-test failed: missing export was not reported")
    finally:
        path.unlink()


def missing_exports(path: Path, needed: list[str]) -> list[str]:
    names = dynsym_names(path)
    return [symbol for symbol in needed if symbol not in names]


def elf64_with_dynsym(symbol: str) -> bytes:
    shstrtab = b"\0.shstrtab\0.dynstr\0.dynsym\0"
    dynstr = b"\0" + symbol.encode("ascii") + b"\0"
    dynsym = bytes(24) + struct.pack("<IBBHQQ", 1, 0x12, 0, 1, 0x1000, 8)

    shstrtab_offset = 64
    dynstr_offset = shstrtab_offset + len(shstrtab)
    dynsym_offset = dynstr_offset + len(dynstr)
    dynsym_pad = (8 - (dynsym_offset % 8)) % 8
    dynsym_offset += dynsym_pad
    section_offset = dynsym_offset + len(dynsym)

    sections = (
        bytes(64)
        + elf64_section(1, 3, 0, shstrtab_offset, len(shstrtab), 0, 0, 1, 0)
        + elf64_section(11, 3, 2, dynstr_offset, len(dynstr), 0, 0, 1, 0)
        + elf64_section(19, 11, 2, dynsym_offset, len(dynsym), 2, 1, 8, 24)
    )
    header = bytes([0x7F, 0x45, 0x4C, 0x46, 2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    header += struct.pack(
        "<HHIQQQIHHHHHH", 3, 62, 1, 0, 0, section_offset, 0, 64, 0, 0, 64, 4, 1
    )
    return header + shstrtab + dynstr + (b"\0" * dynsym_pad) + dynsym + sections


def elf64_section(
    name: int,
    typ: int,
    flags: int,
    offset: int,
    size: int,
    link: int,
    info: int,
    align: int,
    entsize: int,
) -> bytes:
    return struct.pack(
        "<IIQQQQIIQQ", name, typ, flags, 0, offset, size, link, info, align, entsize
    )


def dynsym_names(path: Path) -> set[str]:
    data = path.read_bytes()
    if data[:4] != b"\x7fELF":
        raise SystemExit(f"{path} is not an ELF file")

    layout = elf_layout(data, path)
    dynsym = find_dynsym(data, layout, path)
    return names_from_dynsym(data, layout, dynsym)


def elf_layout(data: bytes, path: Path) -> dict:
    endian = "<" if data[5] == 1 else ">"
    elf_class = data[4]
    if elf_class == 2:
        return {
            "endian": endian,
            "section_header_offset": struct.unpack_from(endian + "Q", data, 40)[0],
            "section_header_size": struct.unpack_from(endian + "H", data, 58)[0],
            "section_count": struct.unpack_from(endian + "H", data, 60)[0],
            "string_table_index": struct.unpack_from(endian + "H", data, 62)[0],
            "section_format": endian + "IIQQQQIIQQ",
            "symbol_format": endian + "IBBHQQ",
        }
    if elf_class == 1:
        return {
            "endian": endian,
            "section_header_offset": struct.unpack_from(endian + "I", data, 32)[0],
            "section_header_size": struct.unpack_from(endian + "H", data, 46)[0],
            "section_count": struct.unpack_from(endian + "H", data, 48)[0],
            "string_table_index": struct.unpack_from(endian + "H", data, 50)[0],
            "section_format": endian + "IIIIIIIIII",
            "symbol_format": endian + "IIIBBH",
        }
    raise SystemExit(f"{path} has unsupported ELF class {elf_class}")


def find_dynsym(data: bytes, layout: dict, path: Path):
    string_header = section_header(data, layout, layout["string_table_index"])
    names = slice_bytes(data, string_header[OFFSET], string_header[SIZE])
    named = None
    typed = None
    for index in range(layout["section_count"]):
        header = section_header(data, layout, index)
        name = names[header[NAME] :].split(b"\x00", 1)[0]
        if name == b".dynsym":
            named = header
            break
        if typed is None and header[TYPE] == 11:
            typed = header
    dynsym = named or typed
    if dynsym is None:
        raise SystemExit(f"{path} has no .dynsym section")
    return dynsym


def names_from_dynsym(data: bytes, layout: dict, dynsym) -> set[str]:
    strings = section_header(data, layout, dynsym[LINK])
    string_table = slice_bytes(data, strings[OFFSET], strings[SIZE])
    entry_size = dynsym[ENTRY_SIZE] or struct.calcsize(layout["symbol_format"])
    names: set[str] = set()
    for index in range(dynsym[SIZE] // entry_size):
        offset = dynsym[OFFSET] + index * entry_size
        symbol = struct.unpack_from(layout["symbol_format"], data, offset)
        raw = string_table[symbol[0] :].split(b"\x00", 1)[0]
        name = raw.decode("utf-8", "replace")
        if name:
            names.add(name)
    return names


def section_header(data: bytes, layout: dict, index: int):
    offset = layout["section_header_offset"] + index * layout["section_header_size"]
    return struct.unpack_from(layout["section_format"], data, offset)


def slice_bytes(data: bytes, offset: int, size: int) -> bytes:
    return data[offset : offset + size]


if __name__ == "__main__":
    main()
