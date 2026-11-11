"""Thin wrapper:  python experiments/run_storage_experiments.py [--quick]   (same as `python -m ttml.cli experiments`)"""
import sys
sys.path.insert(0, "src")
from ttml.cli import main

if __name__ == "__main__":
    main(["experiments", *sys.argv[1:]])
