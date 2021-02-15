"""Tests for pybam module."""

from pybam import BamFile, PybamWarn
import pytest


class TestRead:
    def test_getBamRecordsFromBamFile1Correctly(self):
        bf = BamFile('Tests/Bam/bam1.bam')
        for sequence in bf.getBamRecords():
            assert sequence.bam_qname.decode("utf-8").startswith('HWI-1KL120:88:D0LRBACXX:1:1101:')

    def test_getFirstSequenceFromFile1Correctly(self):
        bf = BamFile('Tests/Bam/bam1.bam')
        records = bf.getBamRecords()
        record = next(records)
        # the known value of the first sequence
        assert record.sam_seq == 'GAGAGGTCAGCGTGAGCCCCTTGCCTCACACCGGCCCCTCTCACGCCGAGAGAGGTCAGCGTGAGCCCCTTGCCTCACACCGGCCCCTCCCACGCCGAGAG'

    def test_getFileChromosomesFromFile1Correctly(self):
        bf = BamFile('Tests/Bam/bam1.bam')
        for sequence in bf.getBamRecords():
            assert sequence.sam_rname(bf.file_chromosomes) == '*' or sequence.sam_rname(bf.file_chromosomes).decode("utf-8") == '1'

    def test_getBamRecordsFromBamFile2Correctly(self):
        bf = BamFile('Tests/Bam/bam2.bam')
        for sequence in bf.getBamRecords():
            assert sequence.bam_qname.decode("utf-8").startswith('HWI-1KL120:88:D0LRBACXX:1:1101:')

    def test_getFirstSequenceFromFile2Correctly(self):
        bf = BamFile('Tests/Bam/bam2.bam')
        records = bf.getBamRecords()
        record = next(records)
        # the known value of the first sequence
        assert record.sam_seq == 'GTTGTTTCTTCCTTTTTTCCCCTCTTCCCCTTCCTGTCTTGTGTTTTTTCTGAACCCCACATCTGCCATCATTTCTCTCCTCTAGAGTCCCTTGCTTCTGC'

    def test_getFileChromosomesFromFile2Correctly(self):
        bf = BamFile('Tests/Bam/bam2.bam')
        for sequence in bf.getBamRecords():
            assert sequence.sam_rname(bf.file_chromosomes) == '*' or sequence.sam_rname(bf.file_chromosomes).decode("utf-8") == '1'

    def test_getBamRecordsFromInvalidBamFileThrows(self):
        bf = BamFile('Tests/Bam/ex1.bam')
        with pytest.raises(PybamWarn):
            for _ in bf.getBamRecords():
                print('help!')
