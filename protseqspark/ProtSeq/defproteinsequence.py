class ProteinSequence:
    def __init__( \
            self, \
            accession: str, \
            geneName: str, \
            specie: str, \
            sequence: str):
        self.accession = accession
        self.geneName = geneName
        self.specie = specie
        self.sequence = sequence
