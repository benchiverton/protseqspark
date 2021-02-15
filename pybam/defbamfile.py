'''
Awesome people who have directly contributed to the project:
Jon Palmer - Bug finder & advice on project direction
Mahmut Uludag - Bug finder

Help:       print pybam.wat
Github:     http://github.com/JohnLonginotto/pybam

This code was written by John Longinotto, a PhD student of the Pospisilik Lab at the Max Planck Institute of Immunbiology & Epigenetics, Freiburg.
My PhD is funded by the Deutsches Epigenom Programm (DEEP), and the Max Planck IMPRS Program.
I study Adipose Biology and Circadian Rhythm in mice, although it seems these days I spend most of my time at the computer :-)

Modifications copyright (C) 2021 Ben Chiverton
'''

import os
import zlib
import subprocess
from struct import unpack
from typing import Iterator

from pybam.defbamentry import BamEntry
from pybam.defexceptions import PybamError, PybamWarn


class BamFile:
    fileName: str

    def __init__(self, f: str):
        self.fileName = f

        self.file_bytes_read = 0
        self.file_chromosomes = []
        self.file_alignments_read = 0
        self.file_chromosome_lengths = {}

    def getBamRecords(self) -> Iterator[BamEntry]:
        gen = self.generator()

        # read the header
        self.readHeader(gen)

        # Variable parsing:
        cache = self.header_cache  # we keep a small cache of X bytes of decompressed BAM data, to smoothen out disk access.
        p = 0  # where the next alignment/entry starts in the cache
        while True:
            try:
                while len(cache) < p + 4:
                    cache = cache[p:] + next(gen)
                    p = 0  # Grab enough bytes to parse blocksize
                self.sam_block_size = unpack('<i', cache[p:p + 4])[0]
                self.file_alignments_read += 1
                while len(cache) < p + 4 + self.sam_block_size:
                    cache = cache[p:] + next(gen)
                    p = 0  # Grab enough bytes to parse entry
                bam = cache[p:p + 4 + self.sam_block_size]
                p = p + 4 + self.sam_block_size
                yield BamEntry(bam)
            except RuntimeError:
                break

    def generator(self):
        DEVNULL = open(os.devnull, 'wb')

        # First we need to figure out what sort of file we have - whether it's gzip compressed, uncompressed, or something else entirely!
        if type(self.fileName) is str:
            try:
                self._file = open(self.fileName, 'rb')
            except:
                raise PybamError(f'\n\nCould not open "{str(self._file.name)}" for reading!\n')
            try:
                magic = os.read(self._file.fileno(), 4)
            except:
                raise PybamError(f'\n\nCould not read from "{str(self._file.name)}"!\n')
        else:
            raise PybamError(f'\n\nInput file was not a string. It was: "{str(self.fileName)}"\n')

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
                self._subprocess = subprocess.Popen(["pigz"], stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL)
                if self._subprocess.returncode is None:
                    self._subprocess.kill()
            except OSError:
                try:
                    self._subprocess = subprocess.Popen(["gzip"], stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL)
                    if self._subprocess.returncode is None:
                        self._subprocess.kill()
                except OSError:
                    pass

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
                magic = raw_data[bs:bs + 4]
                if not magic:
                    break  # a child's heart
                if magic != b'\x1f\x8b\x08\x04':
                    raise PybamError(f'\n\nThe input file is not in a format I understand. First four bytes: {repr(magic)}\n')
                try:
                    more_bs = bs + unpack("<H", raw_data[bs + 16:bs + 18])[0] + 1
                    internal_cache.append(decompress(raw_data[bs + 18:more_bs - 8], -15))
                    bs = more_bs
                except:  # zlib doesnt have a nice exception for when things go wrong. just "error"
                    header_data = magic + raw_data[bs + 4:bs + 12]
                    header_size = 12
                    extra_len = unpack("<H", header_data[-2:])[0]
                    while header_size - 12 < extra_len:
                        header_data += raw_data[bs + 12:bs + 16]
                        subfield_id = header_data[-4:-2]
                        subfield_len = unpack("<H", header_data[-2:])[0]
                        subfield_data = raw_data[bs + 16:bs + 16 + subfield_len]
                        header_data += subfield_data
                        header_size += subfield_len + 4
                        if subfield_id == 'BC':
                            block_size = unpack("<H", subfield_data)[0]
                    raw_data = raw_data[bs + 16 + subfield_len:bs + 16 + subfield_len + block_size - extra_len - 19]
                    crc_data = raw_data[bs + 16 + subfield_len + block_size - extra_len - 19:bs + 16 + subfield_len + block_size - extra_len - 19 + 8]  # I have left the numbers in verbose, because the above try is the optimised code.
                    bs = bs + 16 + subfield_len + block_size - extra_len - 19 + 8
                    zipped_data = header_data + raw_data + crc_data
                    internal_cache.append(decompress(zipped_data, 47))  # 31 works the same as 47.
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
            if internal_cache != b'':
                yield b''.join(internal_cache)
            raise StopIteration

        else:
            raise PybamError(f'\n\nThe input file is not in a format I understand. First four bytes: {repr(magic)}\n')

    def readHeader(self, generator: generator):
        self.header_cache = b''
        while len(self.header_cache) < 8:
            self.header_cache += next(generator)

        p_from = 0
        p_to = 4
        if self.header_cache[p_from:p_to] != b'BAM\1':
            raise PybamError(f'\n\nInput file {self.file_name} does not appear to be a BAM file.\n')

        # Parse the BAM header:
        p_from = p_to
        p_to += 4
        length_of_header = unpack('<i', self.header_cache[p_from:p_to])[0]
        p_from = p_to
        p_to += length_of_header
        while len(self.header_cache) < p_to:
            self.header_cache += next(generator)
        self.file_header = self.header_cache[p_from:p_to]
        p_from = p_to
        p_to += 4
        while len(self.header_cache) < p_to:
            self.header_cache += next(generator)
        number_of_reference_sequences = unpack('<i', self.header_cache[p_from:p_to])[0]

        for _ in range(number_of_reference_sequences):
            p_from = p_to
            p_to += 4
            while len(self.header_cache) < p_to:
                self.header_cache += next(generator)
            l_name = unpack('<l', self.header_cache[p_from:p_to])[0]
            p_from = p_to
            p_to += l_name
            while len(self.header_cache) < p_to:
                self.header_cache += next(generator)
            self.file_chromosomes.append(self.header_cache[p_from:p_to - 1])
            p_from = p_to
            p_to += 4
            while len(self.header_cache) < p_to:
                self.header_cache += next(generator)
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
