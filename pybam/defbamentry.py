from dataclasses import dataclass
from array import array
from struct import unpack

CtoPy = {'A': '<c', 'c': '<b', 'C': '<B', 's': '<h', 'S': '<H', 'i': '<i', 'I': '<I', 'f': '<f'}
py4py = {'A': 1, 'c': 1, 'C': 1, 's': 2, 'S': 2, 'i': 4, 'I': 4, 'f': 4}
cigar_codes = 'MIDNSHP=X'
dna_codes = '=ACMGRSVTWYHKDBN'


# BAM = Binary Alignment Map
# This class contains methods to pull out raw bam data from an entry (so still in its binary encoding). This can be helpful in some scenarios.
@dataclass()
class BamEntry:
    entry: str

    def __init__(self, entry: str):
        self.entry = entry

    @property
    def bam_block_size(self): return self.entry[:4]
    @property
    def bam_refID(self): return self.entry[4:8]
    @property
    def bam_pos(self): return self.entry[8:12]
    @property
    def bam_l_read_name(self): return self.entry[12:13]
    @property
    def bam_mapq(self): return self.entry[13:14]
    @property
    def bam_bin(self): return self.entry[14:16]
    @property
    def bam_n_cigar_op(self): return self.entry[16:18]
    @property
    def bam_flag(self): return self.entry[18:20]
    @property
    def bam_l_seq(self): return self.entry[20:24]
    @property
    def bam_next_refID(self): return self.entry[24:28]
    @property
    def bam_pnext(self): return self.entry[28:32]
    @property
    def bam_tlen(self): return self.entry[32:36]
    @property
    def bam_qname(self): return self.entry[36:self._end_of_qname]
    @property
    def bam_cigar(self): return self.entry[self._end_of_qname:self._end_of_cigar]
    @property
    def bam_seq(self): return self.entry[self._end_of_cigar:self._end_of_seq]
    @property
    def bam_qual(self): return self.entry[self._end_of_seq:self._end_of_qual]
    @property
    def bam_tags(self): return self.entry[self._end_of_qual:]
    @property
    def sam_refID(self): return unpack('<i', self.entry[4:8])[0]
    @property
    def sam_pos0(self): return unpack('<i', self.entry[8:12])[0]
    @property
    def sam_l_read_name(self): return unpack('<B', self.entry[12:13])[0]
    @property
    def sam_mapq(self): return unpack('<B', self.entry[13:14])[0]
    @property
    def sam_bin(self): return unpack('<H', self.entry[14:16])[0]
    @property
    def sam_n_cigar_op(self): return unpack('<H', self.entry[16:18])[0]
    @property
    def sam_flag(self): return unpack('<H', self.entry[18:20])[0]
    @property
    def sam_l_seq(self): return unpack('<i', self.entry[20:24])[0]
    @property
    def sam_next_refID(self): return unpack('<i', self.entry[24:28])[0]
    @property
    def sam_pnext0(self): return unpack('<i', self.entry[28:32])[0]
    @property
    def sam_tlen(self): return unpack('<i', self.entry[32:36])[0]
    @property
    def sam_qname(self): return self.entry[36:self._end_of_qname - 1]  # -1 to remove trailing NUL byte
    @property
    def sam_cigar_list(self): return [(cig >> 4, cigar_codes[cig & 0b1111]) for cig in array('I', self.entry[self._end_of_qname:self._end_of_cigar])]
    @property
    def sam_cigar_string(self): return ''.join([str(cig >> 4) + cigar_codes[cig & 0b1111] for cig in array('I', self.entry[self._end_of_qname:self._end_of_cigar])])
    @property
    def sam_seq(self): return ''.join([dna_codes[dna >> 4] + dna_codes[dna & 0b1111] for dna in array('B', self.entry[self._end_of_cigar:self._end_of_seq])])[:self.sam_l_seq]  # As DNA is 4 bits packed 2-per-byte, there might be a trailing '0000', so we can either
    @property
    def sam_qual(self): return ''.join([chr(ord(quality) + 33) for quality in self.entry[self._end_of_seq:self._end_of_qual]])

    @property
    def sam_tags_list(self):
        result = []
        offset = self._end_of_qual
        while offset != len(self.entry):
            tag_name = self.entry[offset:offset + 2]
            tag_type = self.entry[offset + 2]
            if tag_type == 'Z':
                offset_end = self.entry.index('\x00', offset + 3) + 1
                tag_data = self.entry[offset + 3:offset_end - 1]
            elif tag_type in CtoPy:
                offset_end = offset + 3 + py4py[tag_type]
                tag_data = unpack(CtoPy[tag_type], self.entry[offset + 3:offset_end])[0]
            elif tag_type == 'B':
                offset_end = offset + 8 + (unpack('<i', self.entry[offset + 4:offset + 8])[0] * py4py[self.entry[offset + 3]])
                tag_data = array(self.entry[offset + 3], self.entry[offset + 8:offset_end])
            else:
                print(f'PYBAM ERROR: I dont know how to parse BAM tags in this format: {repr(tag_type)}')
                print('             This is simply because I never saw this kind of tag during development.')
                print('             If you could mail the following chunk of text to john at john.uk.com, ill fix this up :)')
                print(f'{repr(tag_type)}{repr(self.entry[offset+3:offset_end])}')
                exit()
            result.append((tag_name, tag_type, tag_data))
            offset = offset_end
        return result

    @property
    def sam_tags_string(self):
        return '\t'.join(A + ':' + ('i' if B in 'cCsSI' else B) + ':' + ((C.typecode + ',' + ','.join(map(str, C))) if type(C) == array else str(C)) for A, B, C in self.sam_tags_list)

    # BONUS methods - methods that mimic how samtools works.
    @property
    def sam_pos1(self): return 0 if self.sam_pos0 < 0 else self.sam_pos0 + 1
    @property
    def sam_pnext1(self): return 0 if self.sam_pnext0 < 0 else self.sam_pnext0 + 1
    def sam_rname(self, file_chromosomes: list): return '*' if self.sam_refID < 0 else file_chromosomes[self.sam_refID]
    def sam_rnext(self, file_chromosomes: list): return '*' if self.sam_next_refID < 0 else file_chromosomes[self.sam_next_refID]

    @property
    def sam(self): return (
        self.sam_qname + '\t'
        + str(self.sam_flag) + '\t'
        + self.sam_rname + '\t'
        + str(self.sam_pos1) + '\t'
        + str(self.sam_mapq) + '\t'
        + ('*' if self.sam_cigar_string == '' else self.sam_cigar_string) + '\t'
        + ('=' if self.bam_refID == self.bam_next_refID else self.sam_rnext) + '\t'
        + str(self.sam_pnext1) + '\t'
        + str(self.sam_tlen) + '\t'
        + self.sam_seq + '\t'
        + self.sam_qual + '\t'
        + self.sam_tags_string
    )

    # Internal methods - methods used to calculate where variable-length blocks start/end
    @property
    def _end_of_qname(self): return self.sam_l_read_name + 36  # fixed-length stuff at the beginning takes up 36 bytes.
    @property
    def _end_of_cigar(self): return self._end_of_qname + (4 * self.sam_n_cigar_op)  # 4 bytes per n_cigar_op
    @property
    def _end_of_seq(self): return self._end_of_cigar + (-((-self.sam_l_seq) // 2))  # {blurgh}
    @property
    def _end_of_qual(self): return self._end_of_seq + self.sam_l_seq  # qual has the same length as seq
