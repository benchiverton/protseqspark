from typing import Iterator
from ..ProtSeq import ProteinSequence

def writeSequenceToTsv(tsv_file: str, sequences: Iterator[ProteinSequence]):
    file = open(tsv_file, "x")
    for seq in sequences:
        file.write(f'{sequenceToTsv(seq)}\n')
    file.close()

def sequenceToTsv(seq: ProteinSequence) -> str:
    return f'{seq.accession}\t{seq.geneName}\t{seq.specie}\t{seq.sequence}'
    
def sequenceFromTsv(row: str) -> ProteinSequence:
    parts = row.split("\t"); 
    return ProteinSequence(parts[0], parts[1], parts[2], parts[3])