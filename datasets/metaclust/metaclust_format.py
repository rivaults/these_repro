import sys
NB_BLOCKS = 200
BLOCKS_SIZE = 5 * 1024 * 1024 * 1024
MIN_LEN = 100
seq_id = 0

inp = open(sys.argv[1])
file_id = 0
out = open(f"metaclust/{file_id}", "wb+")
current = ""
for i, l in enumerate(inp):
    if not l.startswith(">"):
        current += l.strip().replace('*', '')
    elif l.startswith(">"):
        if MIN_LEN <= len(current):
            out.write(f"{current}\t{seq_id}\n".encode('utf-8'))
            seq_id += 1
        current = ""
        if BLOCKS_SIZE < out.tell():
            file_id += 1
            out.close()
            out = open(f"metaclust/{file_id}", "wb+")
inp.close()
out.close()
