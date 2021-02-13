"""Tests for pybam module."""

import pybam

class TestRead:
    def test_readBamFileCorrectly(self):
        for alignment in pybam.read('Tests/Bam/bam1.bam'):
            print(alignment.sam_seq)