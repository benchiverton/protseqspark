from dataclasses import dataclass

@dataclass()
class ProteinSequence:
    accession: str
    geneName: str
    specie: str
    sequence: str
