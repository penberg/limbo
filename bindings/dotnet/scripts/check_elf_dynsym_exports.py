#!/usr/bin/env python3

import struct
import sys
from pathlib import Path


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

    def section(name: int, typ: int, flags: int, offset: int, size: int, link: int, info: int, align: int, entsize: int) -> bytes:
        return struct.pack("<IIQQQQIIQQ", name, typ, flags, 0, offset, size, link, info, align, entsize)

    sections = (
        bytes(64)
        + section(1, 3, 0, shstrtab_offset, len(shstrtab), 0, 0, 1, 0)
        + section(11, 3, 2, dynstr_offset, len(dynstr), 0, 0, 1, 0)
        + section(19, 11, 2, dynsym_offset, len(dynsym), 2, 1, 8, 24)
    )
    header = bytes([0x7F, 0x45, 0x4C, 0x46, 2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    header += struct.pack("<HHIQQQIHHHHHH", 3, 62, 1, 0, 0, section_offset, 0, 64, 0, 0, 64, 4, 1)
    return header + shstrtab + dynstr + (b"\0" * dynsym_pad) + dynsym + sections


def dynsym_names(path: Path) -> set[str]:
    data = path.read_bytes()
    if data[:4] != b"\x7fELF":
        raise SystemExit(f"{path} is not an ELF file")

    little = data[5] == 1
    endian = "<" if little else ">"
    elf_class = data[4]
    if elf_class == 2:
        section_header_offset = struct.unpack_from(endian + "Q", data, 40)[0]
        section_header_size, section_count, string_table_index = struct.unpack_from(
            endian + "HHH", data, 58
        )
        section_format = endian + "IIQQQQIIQQ"
        name_index, type_index, offset_index, size_index, link_index, entry_size_index = (
            0,
            1,
            4,
            5,
            6,
            9,
        )
        symbol_format = endian + "IBBHQQ"
        symbol_name_index = 0
    elif elf_class == 1:
        section_header_offset = struct.unpack_from(endian + "I", data, 32)[0]
        section_header_size, section_count, string_table_index = struct.unpack_from(
            endian + "HHH", data, 46
        )
        section_format = endian + "IIIIIIIIII"
        name_index, type_index, offset_index, size_index, link_index, entry_size_index = (
            0,
            1,
            4,
            5,
            6,
            9,
        )
        symbol_format = endian + "IIIBBH"
        symbol_name_index = 0
    else:
        raise SystemExit(f"{path} has unsupported ELF class {elf_class}")

    def section_header(index: int):
        return struct.unpack_from(
            section_format, data, section_header_offset + index * section_header_size
        )

    string_header = section_header(string_table_index)
    section_names = data[
        string_header[offset_index] : string_header[offset_index] + string_header[size_index]
    ]

    dynsym = None
    for index in range(section_count):
        header = section_header(index)
        name = section_names[header[name_index] :].split(b"\x00", 1)[0]
        if name == b".dynsym":
            dynsym = header
            break
        if dynsym is None and header[type_index] == 11:
            dynsym = header
    if dynsym is None:
        raise SystemExit(f"{path} has no .dynsym section")

    string_table_header = section_header(dynsym[link_index])
    string_table = data[
        string_table_header[offset_index] : string_table_header[offset_index]
        + string_table_header[size_index]
    ]
    entry_size = dynsym[entry_size_index] or struct.calcsize(symbol_format)
    names: set[str] = set()
    for index in range(dynsym[size_index] // entry_size):
        symbol = struct.unpack_from(symbol_format, data, dynsym[offset_index] + index * entry_size)
        name = string_table[symbol[symbol_name_index] :].split(b"\x00", 1)[0].decode("utf-8", "replace")
        if name:
            names.add(name)
    return names


if __name__ == "__main__":
    main()
