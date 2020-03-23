import sys

# count - how many users will be chosen from the group
group_id, count = map(int, sys.argv[1:3])
if group_id is not None and count is not None:
    from fetcher.fetch import process
    process(group_id, count)
else:
    print('Specify group_id and count!')
