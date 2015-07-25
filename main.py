#!/usr/bin/env python

import re
import os
import os.path
import sys
import cjson
from itertools import groupby, combinations, islice
from collections import namedtuple, defaultdict, Counter
import subprocess

import requests
requests.packages.urllib3.disable_warnings()

import pandas as pd
import numpy as np
import networkx as nx
from matplotlib import pyplot as plt


PUBLIC = 1
GROUP = 2
OPEN = 1
WHATEVER = -1
GROUPS_CHUNKS = 'allsocial'
MEMBERS = 'members'
TABLES = 'tables'
SPLIT = 'split'
TMP_TABLES = os.path.join(TABLES, 'tmp')


def table(name, dir=TABLES):
    return os.path.join(dir, name + '.tsv')


GROUPS_MEMBERS = table('groups_members')
GROUP_COLUMN = 0
USER_COLUMN = 1
COMBINATIONS = table('combinations')
SOURCE = 0
TARGET = 1


# https://oauth.vk.com/authorize?client_id=5006136&redirect_uri=https://oauth.vk.com/blank.html&display=page&response_type=token 
TOKEN = '6bbf6946884c44a7385f26edc06841402405decda514650147aa7b1e226dbec30ec28443a48aaf8b698b0'


def download_groups_chunk(offset=0, type=PUBLIC):
    access = WHATEVER
    if type == GROUP:
        access = OPEN
    response = requests.get(
        'http://allsocial.ru/entity',
        params={
            'direction': 1,
            'is_closed': access,
            'is_verified': -1,
            'list_type': 1,
            'offset': offset,
            'order_by': 'quantity',
            'period': 'month',
            'platform': 1,
            'range': '0:8000000',
            'type_id': type,
        }
    )
    print >>sys.stderr, 'Load offset: {}, type: {}'.format(offset, type)
    data = response.json()
    return data['response']['entity']


def list_groups_chunks_cache(dir=GROUPS_CHUNKS):
    for filename in os.listdir(dir):
        match = re.match(r'(\d)_(\d+).json', filename)
        type, offset = match.groups()
        type = int(type)
        offset = int(offset)
        yield type, offset


def groups_chunk_path(offset, type, dir=GROUPS_CHUNKS):
    filename = '{}_{}.json'.format(type, offset)
    path = os.path.join(dir, filename)
    return path


def load_groups_chunk(offset, type, dir=GROUPS_CHUNKS):
    path = groups_chunk_path(offset, type, dir=dir)
    with open(path) as file:
        return cjson.decode(file.read())


def dump_groups_chunk(chunk, offset, type, dir=GROUPS_CHUNKS):
    path = groups_chunk_path(offset, type, dir=dir)
    with open(path, 'w') as file:
        file.write(cjson.encode(chunk))


def get_groups_chunk(offset=0, type=PUBLIC, dir=GROUPS_CHUNKS):
    cache = set(list_groups_chunks_cache(dir=dir))
    if (type, offset) in cache:
        return load_groups_chunk(offset, type, dir=dir)
    else:
        chunk = download_groups_chunk(offset, type)
        dump_groups_chunk(chunk, offset, type, dir=dir)
        return chunk


def read_groups_list(dir=GROUPS_CHUNKS):
    for type, offset in list_groups_chunks_cache(dir=dir):
        for record in load_groups_chunk(offset, type):
            yield record

            
def load_groups_list(dir=GROUPS_CHUNKS):
    data = pd.DataFrame(read_groups_list(dir=dir))
    data = data.sort('quantity', ascending=False, inplace=False)
    data.index = range(0, len(data))
    return data


class ApiCallError(Exception):
    pass


def call_vk(method, v=5.34, token=TOKEN, **params):
    params.update(v=v, access_token=token)
    response = requests.get(
        'https://api.vk.com/method/' + method,
        params = params
    )
    data = response.json()
    if 'error' in data:
        raise ApiCallError(data['error'])
    else:
        return data['response']


def download_members(id, offset=0, count=1000, fields=(), call_vk=call_vk):
    fields = ','.join(fields)
    return call_vk(
        'groups.getMembers',
        group_id=id,
        offset=offset,
        count=count,
        fields=fields
    )


def list_members_cache(dir=MEMBERS):
    for hash in range(10):
        ids = os.path.join(dir, str(hash))
        for id in os.listdir(ids):
            id = int(id)
            offsets = os.path.join(ids, str(id))
            for offset in os.listdir(offsets):
                offset, _ = offset.split('.', 1)
                offset = int(offset)
                yield id, offset


def members_path(id, offset, dir=MEMBERS):
    path = os.path.join(dir, str(id % 10), str(id), str(offset) + '.json')
    return path


def load_members(id, offset, dir=MEMBERS):
    path = members_path(id, offset, dir=dir)
    with open(path) as file:
        banned, members = cjson.decode(file.read())
        banned = set(banned)
        return banned, members


def dump_members(members, id, offset, dir=MEMBERS):
    path = members_path(id, offset, dir=dir)
    if not os.path.exists(path):
        dir = os.path.dirname(path)
        if not os.path.exists(dir):
            os.mkdir(dir)
    with open(path, 'w') as file:
        file.write(cjson.encode(members))


def in_vkscript(method, **params):
    return 'API.{}({})'.format(method, cjson.encode(params))


def call_for(function, *args, **kwargs):
    return function(*args, call_vk=in_vkscript, **kwargs)


def run_execute(code):
    return call_vk(
        'execute',
        code=code
    )


def run_bulk(calls):
    calls = list(calls)
    print >>sys.stderr, 'Execute:'
    for call in calls:
        print >>sys.stderr, '   ', call
    code = 'return [{}];'.format(','.join(calls))
    return run_execute(code)


MembersFuture = namedtuple('MembersFuture', 'function, id, offset, count, fields')


def generate_download_members_calls(groups):
    for _, row in groups.iterrows():
        id = row.vk_id
        size = row.quantity
        for offset in range(0, size, 1000):
            yield MembersFuture(
                download_members, id, offset, 1000, ('deactivated',)
            )


def group_download_members_calls(calls, size=25):
    for _, group in groupby(enumerate(calls), lambda (index, call): index / size):
        yield [call for _, call in group]


def download_members_bulks(groups, dir=MEMBERS):
    cache = set(list_members_cache(dir=dir))
    bulks = group_download_members_calls(
        generate_download_members_calls(groups)
    )
    for bulk in bulks:
        if any((_.id, _.offset) not in cache for _ in bulk):
            chunks = run_bulk(
                call_for(
                    _.function, _.id,
                    offset=_.offset, count=_.count, fields=_.fields
                )
                for _ in bulk
            )
            for call, members in zip(bulk, chunks):
                banned = []
                users = []
                try:
                    for index, item in enumerate(members['items']):
                        if 'deactivated' in item:
                            banned.append(index)
                        users.append(item['id'])
                except:
                    print >>sys.stderr, ('Bad response for {}, offset: {}'
                                         .format(call.id, call.offset))
                members = (banned, users)
                dump_members(members, call.id, call.offset, dir=dir)


def get_members_download_progress(dir=MEMBERS):
    data = []
    for id, offset in list_members_cache(dir=dir):
        path = members_path(id, offset, dir=dir)
        time = os.path.getmtime(path)
        data.append((id, offset, time))
    data = pd.DataFrame(data, columns=['id', 'offset', 'time'])
    data.time = pd.to_datetime(data.time, unit='s')
    return data


def show_progress(progress):
    table = progress.groupby('id').time.max()
    table = table.sort(inplace=False)
    table = table.reset_index().set_index('time')
    table['groups'] = 1
    table.groups.cumsum().plot()


def read_groups_members(dir=MEMBERS, ignore_banned=True):
    cache = list_members_cache(dir=dir)
    for index, (id, offset) in enumerate(cache):
        banned, members = load_members(id, offset, dir=dir)
        for index, member in enumerate(members):
            if not ignore_banned or index not in banned:
                yield id, member


def serialize_groups_members(stream):
    for id, member in stream:
        yield str(id), str(member)


def deserialize_groups_members(stream):
    for id, member in stream:
        yield int(id), int(member)


def read_table(table):
    with open(table) as file:
        for line in file:
            yield line.rstrip('\n').split('\t')


def write_table(stream, table):
    with open(table, 'w') as file:
        file.writelines('\t'.join(_) + '\n' for _ in stream)


def sort_table(table, by, chunks=20):
    if not isinstance(by, (list, tuple)):
        by = (by,)
    size = get_table_size(table) / chunks
    tmp = os.path.join(TMP_TABLES, SPLIT)
    try:
        print >>sys.stderr, ('Split in {} chunks, prefix: {}'
                             .format(chunks, tmp))
        subprocess.check_call(
            ['split', '-l', str(size), table, tmp]
        )
        ks = ['-k{0},{0}'.format(_ + 1) for _ in by]
        tmps = [os.path.join(TMP_TABLES, _)
                for _ in os.listdir(TMP_TABLES)]
        for index, chunk in enumerate(tmps):
            print >>sys.stderr, 'Sort {}/{}: {}'.format(
                index + 1, chunks, chunk
            )
            subprocess.check_call(
                ['sort'] + ks + ['-o', chunk, chunk]
            )
        print >>sys.stderr, 'Merge into', table
        subprocess.check_call(
            ['sort'] + ks + ['-m'] + tmps + ['-o', table]
        )
    finally:
        for name in os.listdir(TMP_TABLES):
            path = os.path.join(TMP_TABLES, name)
            os.remove(path)


def group_stream(stream, by):
    if isinstance(by, (list, tuple)):
        return groupby(stream, lambda r: [r[_] for _ in by])
    else:
        return groupby(stream, lambda r: r[by])


def stream_size(stream):
    return sum(1 for _ in stream)


def reduce_user_groups(groups):
    for user, group in groups:
        count = stream_size(group)
        yield user, count


def reduce_groups_sizes(groups):
    for group, records in groups:
        size = stream_size(records)
        yield group, size


def log_progress(stream, every=1000, total=None):
    if total:
        every = total / 200     # every 0.5%
    for index, record in enumerate(stream):
        if index % every == 0:
            if total:
                progress = float(index) / total
                progress = '{0:0.2f}%'.format(progress * 100)
            else:
                progress = index
            print >>sys.stderr, progress,
        yield record


def get_table_size(table):
    output = subprocess.check_output(['wc', '-l', table])
    size, _ = output.split(None, 1)
    return int(size)


def reduce_combinations(groups, cap=10):
    for user, records in groups:
        groups = sorted(group for group, _ in records)
        if len(groups) < cap:
            for source, target in combinations(groups, 2):
                yield source, target


def serialize_combinations(stream):
    for source, target in stream:
        yield str(source), str(target)


def deserialize_combinations(stream):
    for source, target in stream:
        yield int(source), int(target)


def reduce_edges(groups, sizes):
    for (source, target), records in groups:
        intersection = stream_size(records)
        yield source, target, float(intersection) / sizes[source]
        yield target, source, float(intersection) / sizes[target]


def build_graph(edges, sizes):
    nodes = set()
    for source, target, weight in edges:
        nodes.add(source)
        nodes.add(target)

    graph = nx.DiGraph()
    for node in nodes:
        graph.add_node(node, weight=sizes[node])
    for source, target, weight in edges:
        graph.add_edge(source, target, weight=weight)
    return graph


def save_graph(graph, path='graph.gexf'):
    nx.write_gexf(graph, path)


if __name__ == '__main__':
    groups = load_groups_list()
    slice = groups[(groups.quantity > 5000) & (groups.quantity < 10000)].iloc[::-1]
    download_members_bulks(slice)
