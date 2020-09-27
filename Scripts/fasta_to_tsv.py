import sys
import os
from protseqspark import ProtSeqIO

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""
        Usage: fasta_to_tsv.py <fasta_file>

        Assumes you have a fasta file stored in <fasta_file>.
        """, file=sys.stderr)
        sys.exit(-1)

    fasta_file = sys.argv[1]
    pre, ext = os.path.splitext(fasta_file)
    tsv_file = f'{pre}.tsv'

    ProtSeqIO.writeSequenceToTsv(
        tsv_file, 
        ProtSeqIO.readSequencesFromFasta(fasta_file))
