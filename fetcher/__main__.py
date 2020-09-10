import argparse

from fetcher.process import init_and_run

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Prepare dataset for ml')
    parser.add_argument('--skip-fetcher', action='store_true')
    parser.add_argument('--skip-merger', action='store_true')
    parser.add_argument('--skip-ml', action='store_true')
    parser.add_argument('--model-path', help='path to the fasttext model')
    args = parser.parse_args()
    init_and_run(**vars(args))
