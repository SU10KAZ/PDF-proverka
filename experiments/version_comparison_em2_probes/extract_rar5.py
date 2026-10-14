"""Распаковка RAR5 через libarchive.so.13 (системный 7z без кодека RAR5). Аргументы: <архив> <пустая папка назначения>. Запуск: python -I.

Архив не изменяется; пути внутри архива проверяются на выход за папку назначения.
См. docs/version_comparison_plan.md §16.
"""
import ctypes, ctypes.util, os, sys
lib = ctypes.CDLL("libarchive.so.13")
lib.archive_read_new.restype = ctypes.c_void_p
lib.archive_read_support_format_all.argtypes=[ctypes.c_void_p]
lib.archive_read_support_filter_all.argtypes=[ctypes.c_void_p]
lib.archive_read_open_filename.argtypes=[ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
lib.archive_read_next_header.argtypes=[ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
lib.archive_entry_pathname_utf8.argtypes=[ctypes.c_void_p]; lib.archive_entry_pathname_utf8.restype=ctypes.c_char_p
lib.archive_entry_pathname.argtypes=[ctypes.c_void_p]; lib.archive_entry_pathname.restype=ctypes.c_char_p
lib.archive_entry_filetype.argtypes=[ctypes.c_void_p]; lib.archive_entry_filetype.restype=ctypes.c_uint
lib.archive_read_data.argtypes=[ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]; lib.archive_read_data.restype=ctypes.c_ssize_t
lib.archive_error_string.argtypes=[ctypes.c_void_p]; lib.archive_error_string.restype=ctypes.c_char_p
src, dst = sys.argv[1], sys.argv[2]
a = lib.archive_read_new()
lib.archive_read_support_format_all(a); lib.archive_read_support_filter_all(a)
r = lib.archive_read_open_filename(a, src.encode(), 1<<20)
assert r == 0, lib.archive_error_string(a)
e = ctypes.c_void_p(); n=0
buf = ctypes.create_string_buffer(1<<20)
dstr = os.path.realpath(dst)
while lib.archive_read_next_header(a, ctypes.byref(e)) in (0, -20):
    p = lib.archive_entry_pathname_utf8(e) or lib.archive_entry_pathname(e)
    name = p.decode('utf-8')
    out = os.path.realpath(os.path.join(dst, name))
    assert out.startswith(dstr + os.sep), name
    if lib.archive_entry_filetype(e) == 0o040000:
        os.makedirs(out, exist_ok=True); continue
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'wb') as f:
        while True:
            k = lib.archive_read_data(a, buf, len(buf))
            if k < 0: raise SystemExit(f"err {name}: {lib.archive_error_string(a)}")
            if k == 0: break
            f.write(buf.raw[:k])
    n += 1
print("extracted", n)
