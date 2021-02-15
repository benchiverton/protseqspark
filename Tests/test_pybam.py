"""Tests for pybam module."""

from pybam import BamFile, BamEntry, PybamWarn
from typing import Iterator
import pytest

class TestRead:
    def test_getBamRecordsFromBamFile1Correctly(self):
        bf = BamFile('Tests/Bam/bam1.bam')
        for sequence in bf.getBamRecords():
            assert sequence.bam_qname.decode("utf-8").startswith('HWI-1KL120:88:D0LRBACXX:1:1101:') == True

    def test_getFileChromosomesFromFile1Correctly(self):
        bf = BamFile('Tests/Bam/bam1.bam')
        for sequence in bf.getBamRecords():
            assert sequence.sam_rname(bf.file_chromosomes) == '*' or sequence.sam_rname(bf.file_chromosomes).decode("utf-8") == '1'

    def test_getBamRecordsFromBamFile2Correctly(self):
        bf = BamFile('Tests/Bam/bam2.bam')
        for sequence in bf.getBamRecords():
            assert sequence.bam_qname.decode("utf-8").startswith('HWI-1KL120:88:D0LRBACXX:1:1101:') == True

    def test_getFileChromosomesFromFile2Correctly(self):
        bf = BamFile('Tests/Bam/bam2.bam')
        for sequence in bf.getBamRecords():
            assert sequence.sam_rname(bf.file_chromosomes) == '*' or sequence.sam_rname(bf.file_chromosomes).decode("utf-8") == '1'

    def test_getBamRecordsFromInvalidBamFileThrows(self):
        bf = BamFile('Tests/Bam/ex1.bam')
        with pytest.raises(PybamWarn):
            for _ in bf.getBamRecords():
                print('help!')
