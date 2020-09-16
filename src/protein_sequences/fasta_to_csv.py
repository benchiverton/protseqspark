import sys
import os

from Bio import SeqIO
from models.sequence import Sequence

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""
        Usage: fasta_to_csv.py <fasta_file>

        Assumes you have a fasta file stored in <fasta_file>.
        """, file=sys.stderr)
        sys.exit(-1)

    fasta_file = sys.argv[1]
    pre, ext = os.path.splitext(fasta_file)
    tsv_file = f'{pre}.tsv'

    file = open(tsv_file, "x")

    for seq_record in SeqIO.parse(fasta_file, "fasta"):
        header = str(seq_record.id).split('|')
        accessionSpecie = str(header[2]).split("_")
        seq = Sequence(header[1], accessionSpecie[0], accessionSpecie[1], seq_record.seq)
        file.write(seq.toTsv())

    file.close()