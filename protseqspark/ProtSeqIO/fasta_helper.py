from typing import Iterator
from Bio import SeqIO
from ..ProtSeq import ProteinSequence


def readSequencesFromFasta(fasta_file: str) -> Iterator[ProteinSequence]:
    for seq_record in SeqIO.parse(fasta_file, "fasta"):
        header = str(seq_record.id).split('|')
        accessionSpecie = str(header[2]).split("_")
        yield ProteinSequence(
            header[1],
            accessionSpecie[0],
            accessionSpecie[1],
            seq_record.seq)
