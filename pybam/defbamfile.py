'''
Awesome people who have directly contributed to the project:
Jon Palmer - Bug finder & advice on project direction
Mahmut Uludag - Bug finder

Help:       print pybam.wat
Github:     http://github.com/JohnLonginotto/pybam

This code was written by John Longinotto, a PhD student of the Pospisilik Lab at the Max Planck Institute of Immunbiology & Epigenetics, Freiburg.
My PhD is funded by the Deutsches Epigenom Programm (DEEP), and the Max Planck IMPRS Program.
I study Adipose Biology and Circadian Rhythm in mice, although it seems these days I spend most of my time at the computer :-)
'''

import os
import sys
import zlib
import time
import tempfile
import subprocess
from array import array
from struct import unpack
from typing import Iterator

from pybam.defbamentry import BamEntry
from pybam.defexceptions import PybamError, PybamWarn

parse_codes = {
    'sam':                     ' The current alignment in SAM format.',
    'bam':                     ' All the bytes that make up the current alignment ("read"),\n                              still in binary just as it was in the BAM file. Useful\n                              when creating a new BAM file of filtered alignments.',
    'sam_qname':               ' [1st column in SAM] The QNAME (fragment ID) of the alignment.',
    'bam_qname':               ' The original bytes before decoding to sam_qname.',
    'sam_flag':                ' [2nd column in SAM] The FLAG number of the alignment.',
    'bam_flag':                ' The original bytes before decoding to sam_flag.',
    'sam_refID':               ' The chromosome ID (not the same as the name!).\n                              Chromosome names are stored in the BAM header (file_chromosomes),\n                              so to convert refIDs to chromsome names one needs to do:\n                              "my_bam.file_chromosomes[read.sam_refID]" (or use sam_rname)\n                              But for comparisons, using the refID is much faster that using\n                              the actual chromosome name (for example, when reading through a\n                              sorted BAM file and looking for where last_refID != this_refID)\n                              Note that when negative the alignment is not aligned, and thus one\n                              must not perform my_bam.file_chromosomes[read.sam_refID]\n                              without checking that the value is positive first.',
    'sam_rname':               ' [3rd column in SAM] The actual chromosome/contig name for the\n                              alignment. Will return "*" if refID is negative.',
    'bam_refID':               ' The original bytes before decoding to sam_refID.',
    'sam_pos1':                ' [4th column in SAM] The 1-based position of the alignment. Note\n                              that in SAM format values less than 1 are converted to "0" for\n                              "no data" and sam_pos1 will also do this.',
    'sam_pos0':                ' The 0-based position of the alignment. Note that in SAM all\n                              positions are 1-based, but in BAM they are stored as 0-based.\n                              Unlike sam_pos1, negative values are kept as negative values,\n                              essentially giving one the decoded value as it was stored.',
    'bam_pos':                 ' The original bytes before decoding to sam_pos*.',
    'sam_mapq':                ' [5th column in SAM] The Mapping Quality of the current alignment.',
    'bam_mapq':                ' The original bytes before decoding to sam_mapq.',
    'sam_cigar_string':        ' [6th column in SAM] The CIGAR string, as per the SAM format.\n                              Allowed values are "MIDNSHP=X".',
    'sam_cigar_list':          ' A list of tuples with 2 values per tuple:\n                              the number of bases, and the CIGAR operation applied to those\n                              bases. Faster to calculate than sam_cigar_string.',
    'bam_cigar':               ' The original bytes before decoding to sam_cigar_*.',
    'sam_next_refID':          ' The sam_refID of the alignment\'s mate (if any). Note that as per\n                              sam_refID, this value can be negative and is not the actual\n                              chromosome name (see sam_pnext1).',
    'sam_rnext':               ' [7th column in SAM] The chromosome name of the alignment\'s mate.\n                              Value is "*" if unmapped. Note that in a SAM file this value\n                              is "=" if it is the same as the sam_rname, however pybam will\n                              only do this if the user prints the whole SAM entry with "sam".',
    'bam_next_refID':          ' The original bytes before decoding to sam_next_refID.',
    'sam_pnext1':              ' [8th column in SAM] The 1-based position of the alignment\'s mate.\n                              Note that in SAM format values less than 1 are converted to "0"\n                              for "no data", and sam_pnext1 will also do this.',
    'sam_pnext0':              ' The 0-based position of the alignment\'s mate. Note that in SAM all\n                              positions are 1-based, but in BAM they are stored as 0-based.\n                              Unlike sam_pnext1, negative values are kept as negative values\n                              here, essentially giving you the value as it was stored in BAM.',
    'bam_pnext':               ' The original bytes before decoding to sam_pnext0.',
    'sam_tlen':                ' [9th column in SAM] The TLEN value.',
    'bam_tlen':                ' The original bytes before decoding to sam_tlen.',
    'sam_seq':                 ' [10th column in SAM] The SEQ value (DNA sequence of the alignment).\n                              Allowed values are "ACGTMRSVWYHKDBN and =".',
    'bam_seq':                 ' The original bytes before decoding to sam_seq.',
    'sam_qual':                ' [11th column in SAM] The QUAL value (quality scores per DNA base\n                              in SEQ) of the alignment.',
    'bam_qual':                ' The original bytes before decoding to sam_qual.',
    'sam_tags_list':           ' A list of tuples with 3 values per tuple: a two-letter TAG ID, the\n                              type code used to describe the data in the TAG value (see SAM spec.\n                              for details), and the value of the TAG. Note that the BAM format\n                              has type codes like "c" for a number in the range -127 to +127,\n                              and "C" for a number in the range of 0 to 255.\n                              In a SAM file however, all numerical codes appear to just be stored\n                              using "i", which is a number in the range -2147483647 to +2147483647.\n                              sam_tags_list will therefore return the code used in the BAM file,\n                              and not "i" for all numbers.',
    'sam_tags_string':         ' [12th column a SAM] Returns the TAGs in the same format as would be found \n                              in a SAM file (with all numbers having a signed 32bit code of "i").',
    'bam_tags':                ' The original bytes before decoding to sam_tags_*.',
    'sam_bin':                 ' The bin value of the alignment (used for indexing reads).\n                              Please refer to section 5.3 of the SAM spec for how this\n                              value is calculated.',
    'bam_bin':                 ' The original bytes before decoding to sam_bin.',
    'sam_block_size':          ' The number of bytes the current alignment takes up in the BAM\n                              file minus the four bytes used to store the block_size value\n                              itself. Essentially sam_block_size +4 == bytes needed to store\n                              the current alignment.',
    'bam_block_size':          ' The original bytes before decoding to sam_block_size.',
    'sam_l_read_name':         ' The length of the QNAME plus 1 because the QNAME is terminated\n                              with a NUL byte.',
    'bam_l_read_name':         ' The original bytes before decoding to sam_l_read_name.',
    'sam_l_seq':               ' The number of bases in the seq. Useful if you just want to know\n                              how many bases are in the SEQ but do not need to know what those\n                              bases are (which requires more decoding effort).',
    'bam_l_seq':               ' The original bytes before decoding to sam_l_seq.',
    'sam_n_cigar_op':          ' The number of CIGAR operations in the CIGAR field. Useful if one\n                              wants to know how many CIGAR operations there are, but does not\n                              need to know what they are.',
    'bam_n_cigar_op':          ' The original bytes before decoding to sam_n_cigar_op.',
    'file_alignments_read':    ' A running counter of the number of alignments ("reads"),\n                              processed thus far. Note the BAM format does not store\n                              how many reads are in a file, so the usefulness of this\n                              metric is somewhat limited unless one already knows how\n                              many reads are in the file.',
    'file_binary_header':      ' From the first byte in the file, until the first byte of\n                              the first read. The original binary header.',
    'file_bytes_read':         ' A running counter of the bytes read from the file. Note\n                              that as data is read in arbitary chunks, this is literally\n                              the amount of data read from the file/pipe by pybam.',
    'file_chromosome_lengths': ' The binary header of the BAM file includes chromosome names\n                              and chromosome lengths. This is a dictionary of chromosome-name\n                              keys and chromosome-length values.',
    'file_chromosomes':        ' A list of chromosomes from the binary header.',
    'file_decompressor':       ' BAM files are compressed with bgzip. The value here reflects\n                              the decompressor used. "internal" if pybam\'s internal\n                              decompressor is being used, "gzip" or "pigz" if the system\n                              has these binaries installed and pybam can find them.\n                              Any other value reflects a custom decompression command.',
    'file_directory':          ' The directory the input BAM file can be found in. This will be\n                              correct if the input file is specified via a string or python\n                              file object, however if the input is a pipe such as sys.stdin, \n                              then the current working directory will be used.',
    'file_header':             ' The ASCII portion of the BAM header. This is the typical header\n                              users of samtools will be familiar with.',
    'file_name':               ' The file name (base name) of input file if input is a string or\n                              python file object. If input is via stdin this will be "<stdin>"'
}

class BamFile:
    fileName: str

    file_bytes_read: int
    file_chromosomes: list
    file_alignments_read: int
    file_chromosome_lengths: dict

    header_cache: bytes
    cache: bytes
    sam_block_size: int

    def __init__(self, f: str):
        self.fileName = f

        self.file_bytes_read         = 0
        self.file_chromosomes        = []
        self.file_alignments_read    = 0
        self.file_chromosome_lengths = {}

    def getBamRecords(self) -> Iterator[BamEntry]:
        gen = self.generator()

        # read the header
        self.readHeader(gen)

        ## Variable parsing:
        cache = self.header_cache # we keep a small cache of X bytes of decompressed BAM data, to smoothen out disk access.
        p = 0 # where the next alignment/entry starts in the cache
        while True:
            try:
                while len(cache) < p + 4: 
                    cache = cache[p:] + next(gen)
                    p = 0 # Grab enough bytes to parse blocksize
                self.sam_block_size  = unpack('<i', cache[p:p+4])[0]
                self.file_alignments_read += 1
                while len(cache) < p + 4 + self.sam_block_size:
                    cache = cache[p:] + next(gen)
                    p = 0 # Grab enough bytes to parse entry
                bam = cache[p:p + 4 + self.sam_block_size]
                p = p + 4 + self.sam_block_size
                yield BamEntry(bam)
            except RuntimeError: break

    def generator(self):
        DEVNULL = open(os.devnull, 'wb')

        # First we need to figure out what sort of file we have - whether it's gzip compressed, uncompressed, or something else entirely!
        if type(self.fileName) is str:
            try: self._file = open(self.fileName, 'rb')
            except: raise PybamError(f'\n\nCould not open "{str(self._file.name)}" for reading!\n')
            try: magic = os.read(self._file.fileno(),4)
            except: raise PybamError(f'\n\nCould not read from "{str(self._file.name)}"!\n')
        else: raise PybamError(f'\n\nInput file was not a string. It was: "{str(self.fileName)}"\n')

        self.file_name = os.path.basename(os.path.realpath(self._file.name))
        self.file_directory = os.path.dirname(os.path.realpath(self._file.name))

        if magic == b'BAM\1':
            # The user has passed us already unzipped BAM data! Job done :)
            data = b'BAM\1' + self._file.read(35536)
            self.file_bytes_read += len(data)
            self.file_decompressor = 'None'
            while data:
                yield data
                data = self._file.read(35536)
                self.file_bytes_read += len(data)
            self._file.close()
            DEVNULL.close()
            raise StopIteration

        elif magic == b'\x1f\x8b\x08\x04':
            try:
                self._subprocess = subprocess.Popen(["pigz"],stdin=DEVNULL,stdout=DEVNULL,stderr=DEVNULL)
                if self._subprocess.returncode is None: self._subprocess.kill()
            except OSError:
                try:
                    self._subprocess = subprocess.Popen(["gzip"],stdin=DEVNULL,stdout=DEVNULL,stderr=DEVNULL)
                    if self._subprocess.returncode is None: self._subprocess.kill()
                except OSError: pass

            # Python's gzip module can't read from a stream that doesn't support seek(), and the zlib module cannot read the bgzip format without a lot of help:
            self.file_decompressor = 'internal'
            raw_data = magic + self._file.read(65536)
            self.file_bytes_read = len(raw_data)
            internal_cache = []
            blocks_left_to_grab = 50
            bs = 0
            decompress = zlib.decompress
            while raw_data:
                if len(raw_data) - bs < 35536:
                    raw_data = raw_data[bs:] + self._file.read(65536)
                    self.file_bytes_read += len(raw_data) - bs
                    bs = 0
                magic = raw_data[bs:bs+4]
                if not magic: break # a child's heart
                if magic != b'\x1f\x8b\x08\x04': raise PybamError(f'\n\nThe input file is not in a format I understand. First four bytes: {repr(magic)}\n')
                try:
                    more_bs = bs + unpack("<H", raw_data[bs+16:bs+18])[0] +1
                    internal_cache.append(decompress(raw_data[bs+18:more_bs-8],-15))
                    bs = more_bs
                except: ## zlib doesnt have a nice exception for when things go wrong. just "error"
                    header_data = magic + raw_data[bs+4:bs+12]
                    header_size = 12
                    extra_len = unpack("<H", header_data[-2:])[0]
                    while header_size-12 < extra_len:
                        header_data += raw_data[bs+12:bs+16]
                        subfield_id = header_data[-4:-2]
                        subfield_len = unpack("<H", header_data[-2:])[0]
                        subfield_data = raw_data[bs+16:bs+16+subfield_len]
                        header_data += subfield_data
                        header_size += subfield_len + 4
                        if subfield_id == 'BC': block_size = unpack("<H", subfield_data)[0]
                    raw_data = raw_data[bs+16+subfield_len:bs+16+subfield_len+block_size-extra_len-19]
                    crc_data = raw_data[bs+16+subfield_len+block_size-extra_len-19:bs+16+subfield_len+block_size-extra_len-19+8] # I have left the numbers in verbose, because the above try is the optimised code.
                    bs = bs+16+subfield_len+block_size-extra_len-19+8
                    zipped_data = header_data + raw_data + crc_data
                    internal_cache.append(decompress(zipped_data,47)) # 31 works the same as 47.
                    # Although the following in the bgzip code from biopython, its not needed if you let zlib decompress the whole zipped_data, header and crc, because it checks anyway (in C land)
                    # I've left the manual crc checks in for documentation purposes:
                    '''
                    expected_crc = crc_data[:4]
                    expected_size = unpack("<I", crc_data[4:])[0]
                    if len(unzipped_data) != expected_size: print 'ERROR: Failed to unpack due to a Type 1 CRC error. Could the BAM be corrupted?'; exit()
                    crc = zlib.crc32(unzipped_data)
                    if crc < 0: crc = pack("<i", crc)
                    else:       crc = pack("<I", crc)
                    if expected_crc != crc: print 'ERROR: Failed to unpack due to a Type 2 CRC error. Could the BAM be corrupted?'; exit()
                    '''
                blocks_left_to_grab -= 1
                if blocks_left_to_grab == 0:
                    yield b''.join(internal_cache)
                    internal_cache = []
                    blocks_left_to_grab = 50
            self._file.close()
            DEVNULL.close()
            if internal_cache != b'': yield b''.join(internal_cache)
            raise StopIteration

        else:
            raise PybamError(f'\n\nThe input file is not in a format I understand. First four bytes: {repr(magic)}\n')

    def readHeader(self, generator: generator):
        self.header_cache = b''
        while len(self.header_cache) < 8: self.header_cache += next(generator)

        p_from = 0; p_to = 4
        if self.header_cache[p_from:p_to] != b'BAM\1':
            raise PybamError(f'\n\nInput file {self.file_name} does not appear to be a BAM file.\n')

        ## Parse the BAM header:
        p_from = p_to; p_to += 4
        length_of_header = unpack('<i', self.header_cache[p_from:p_to])[0]
        p_from = p_to; p_to += length_of_header
        while len(self.header_cache) < p_to: self.header_cache += next(generator)
        self.file_header = self.header_cache[p_from:p_to]
        p_from = p_to; p_to += 4
        while len(self.header_cache) < p_to: self.header_cache += next(generator)
        number_of_reference_sequences = unpack('<i', self.header_cache[p_from:p_to])[0]
        
        for _ in range(number_of_reference_sequences):
            p_from = p_to; p_to += 4
            while len(self.header_cache) < p_to: self.header_cache += next(generator)
            l_name = unpack('<l', self.header_cache[p_from:p_to])[0]
            p_from = p_to; p_to += l_name
            while len(self.header_cache) < p_to: self.header_cache += next(generator)
            self.file_chromosomes.append(self.header_cache[p_from:p_to -1])
            p_from = p_to; p_to += 4
            while len(self.header_cache) < p_to: self.header_cache += next(generator)
            self.file_chromosome_lengths[self.file_chromosomes[-1]] = unpack('<l', self.header_cache[p_from:p_to])[0]

        self.file_bytes_read = p_to
        self.file_binary_header = memoryview(self.header_cache[:p_to])
        self.header_cache = self.header_cache[p_to:]

        # A quick check to make sure the header of this BAM file makes sense:
        chromosomes_from_header = []
        for line in self.file_header.split(b'\n'):
            if line.startswith(b'@SQ\tSN:'):
                chromosomes_from_header.append(line.split(b'\t')[1][3:])
        if chromosomes_from_header != self.file_chromosomes:
            raise PybamWarn(f'For some reason the BAM format stores the chromosome names in two locations,\n       the ASCII text header we all know and love, viewable with samtools view -H, and another special binary header\n       which is used to translate the chromosome refID (a number) into a chromosome RNAME when you do bam -> sam.\n\nThese two headers should always be the same, but apparently they are not:\nThe ASCII header looks like: {self.file_header}\nWhile the binary header has the following chromosomes: {self.file_chromosomes}\n')
