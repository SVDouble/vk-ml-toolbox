import json
import glob
import sys
import uuid
from pathlib import Path

CHUNK_SIZE = 1000


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def merge(input_paths, output_path, index_path):
    # load indexes
    print('Loading indexes')
    indexes = set()
    for index in glob.glob(index_path + '*.index.json'):
        with open(index, "r") as f:
            indexes.update(json.load(f))
    print('Total indexes: {}'.format(len(indexes)))

    # load content
    print('Loading files')
    result = []
    num = 1
    for ipath in input_paths:
        print('{} - from path {}'.format(num, ipath))
        for filename in glob.glob(ipath + '*.json'):
            try:
                with open(filename, "r") as f:
                    result.append(json.load(f))
            except:
                pass
        print('Total entries: {}'.format(len(result)))

    # get all ids and filter them
    print('Filtering ids')
    ids = set(map(lambda x: x['id'], result))
    ids.difference_update(indexes)
    print('Ids to save: {}'.format(len(ids)))

    # filter all entries
    print('Getting entries')
    result = list(filter(lambda e: e['id'] in ids, result))

    # save chunks
    num = 1
    print('Saving chunks')
    for chunk in chunks(result, CHUNK_SIZE):
        keys = list(map(lambda x: x['id'], chunk))
        assert keys
        chunk_name = uuid.uuid4().hex
        with open(output_path + chunk_name + '.json', "w") as file:
            json.dump(chunk, file)
        with open(index_path + chunk_name + '.index.json', "w") as file:
            json.dump(keys, file)
        print('{} - {} ready!'.format(num, chunk_name))
        num += 1


if __name__ == '__main__':
    postfix = sys.argv[1]
    assert postfix
    p_index = './index/%s/' % postfix
    p_output = './merged/%s/' % postfix
    p_input = ['.']
    p_input = list(map(lambda p: '%s/%s/' % (p, postfix), p_input))

    paths = [p_index, p_output, *p_input]
    print('Paths: ', paths)
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)
    merge(p_input, p_output, p_index)
