import re
import uuid

import redis

STOP_WORDS = set('''able about across after all almost also am among an 
and any are as st be because been but by can cannot could dear 
did do does either else ever every for from get got has had have he 
her hers him his how however if in into is it its just least let like 
likely may me might most must my neither no nor not of off often on 
only or other out own rather said say says she should since so some 
than that the their them then there these they this tis to too twas 
us wants was we were what when where which while who whom why will 
with would yet you your'''.split())


# 定义单词只能由英文字母和单引号组成
# 注意：这里r"[a-z']{2,}" 大括号中2逗号后面不能带空格否则什么结果都匹配不到
WORDS_RE = re.compile(r"[a-z']{2,}")

def tokenize(content):
    """
    标记化函数
    :param content:
    :return:
    """
    words = set()
    for match in WORDS_RE.finditer(content.lower()):
        word = match.group().strip("'")

        if len(word) >= 2:
            words.add(word)
    return words - STOP_WORDS


def index_document(conn, docid, content):
    """
    索引函数
    :param conn:
    :param docid:
    :param content:
    :return:
    """
    words = tokenize(content)
    pipeline = conn.pipeline(True)
    for word in words:
        pipeline.sadd('idx:' + word, docid)
    return len(pipeline.execute())


def _set_common(conn, method, names, ttl=60, execute=True):
    """
    基本的交，并，差集计算函数
    :param conn:
    :param method:
    :param names: 需要进行交，并，差集计算的多个集合的集合的key
    :param ttl:
    :param execute:
    :return:
    """
    id = str(uuid.uuid4())
    pipeline = conn.pipeline(True) if execute else conn
    names = ['idx:' + name for name in names]
    print('debug names:', names)
    func = getattr(pipeline, method)
    func('idx:' + id, *names)
    pipeline.expire('idx:' + id, ttl)
    if execute:
        pipeline.execute()
    return id


def intersect(conn, items, ttl=30, _execute=True):
    return _set_common(conn, 'sinterstore', items, ttl, _execute)


def union(conn, items, ttl=30, _execute=True):
    return _set_common(conn, 'sunionstore', items, ttl, _execute)


def difference(conn, items, ttl=30, _execute=True):
    return _set_common(conn, 'sdiffstore', items, ttl, _execute)


QUERY_RE = re.compile("[+-]?[a-z']{2,}")


def parse(query):
    unwanted = set()
    all = []
    current = set()
    for match in QUERY_RE.finditer(query.lower()):
        word = match.group()
        prefix = word[:1]
        if prefix in '+-':
            word = word[1:]
        else:
            prefix = None

        word = word.strip("'")
        if len(word) < 2 or word in STOP_WORDS:
            continue
        if prefix == '-':
            unwanted.add(word)
            continue
        if current and not prefix:
            all.append(list(current))
            current = set()
        current.add(word)
    if current:
        all.append(list(current))

    return all, list(unwanted)


def parse_and_search(conn, query, ttl=60):
    """
    分析查询语句并搜索文档,返回搜索结果
    :param conn:
    :param query:
    :param ttl:
    :return:
    """
    all, unwanted = parse(query)
    if not all:
        return None

    to_intersect = []
    for syn in all:
        if len(syn) > 1:
            # 将同义词的结果进行并集计算
            to_intersect.append(union(conn, syn, ttl=ttl))
        else:
            to_intersect.append(syn[0])

    # 如果想要查询的结果有非同义词例如query1这种情形
    if len(to_intersect) > 1:
        # 将所有想要搜索的进行交集计算
        intersect_result = intersect(conn, to_intersect, ttl=ttl)
    else:
        intersect_result = to_intersect[0]
    if unwanted:
        # 用所有想要的结果集合减去不想要的结果集合 intersect_result - unwanted
        unwanted.insert(0, intersect_result)
        return difference(conn, unwanted, ttl=ttl)

    return intersect_result


def search_and_sort(conn, query, id=None, ttl=300, sort='-updated', start=0, num=20):
    desc = sort.startswith('-')
    sort = sort.lstrip('-')
    by = 'kb:doc:*->' + sort
    alpha = sort not in ('updated', 'id', 'created')
    if id and not conn.expire(id, ttl):
        id = None
    if not id:
        id = parse_and_search(conn, query, ttl=ttl)

    pipeline = conn.pipeline(True)
    pipeline.scard('idx:' + id)
    pipeline.sort('idx:' + id, by=by, alpha=alpha, desc=desc, start=start, num=num)
    results = pipeline.execute()

    return results[0], results[1], id


##########################################################################
# 职位搜索
##########################################################################

def add_job(conn, job_id, required_skills):
    conn.sadd('job:' + job_id, **required_skills)


def is_qualified(conn, job_id, candidate_skills):
    """
    通过检查求职者是否具备职位所需的全部技能来判断求职者是否能胜任该职位
    缺点是为了找出求职者适合的所有职位，程序必须对每个职位进行单独的检查
    :param conn:
    :param job_id:
    :param candidate_skills:
    :return:
    """
    temp = str(uuid.uuid4())
    pipeline = conn.pipeline(True)
    pipeline.sadd(temp, **candidate_skills)
    pipeline.expire(5)
    pipeline.sdiff('job:' + job_id, temp)

    return not pipeline.execute()[-1]


def _zset_common(conn, method, scores, ttl=30, **kw):
    id = str(uuid.uuid4())
    execute = kw.pop('_execute', True)
    pipeline = conn.pipeline(True) if execute else conn
    for key in scores.keys():
        scores['idx' + key] = scores.pop(key)
    getattr(pipeline, method)('idx:' + id, scores, **kw)
    pipeline.expire('idx:' + id, ttl)
    if execute:
        pipeline.execute()

    return id


def zintersect(conn, items, ttl=30, **kw):
    return _zset_common(conn, 'zinterscore', dict(items), ttl, **kw)


def zunion(conn, items, ttl=30, **kw):
    return _zset_common(conn, 'zunionstore', dict(items), ttl, **kw)


def index_job(conn, job_id, skills):
    """
    根据所需技能对职位进行索引
    :param conn:
    :param job_id:
    :param skills:
    :return:
    """
    pipeline = conn.pipeline(True)
    for skill in skills:
        pipeline.sadd('idx:skill:' + skill, job_id)
    pipeline.zadd('idx:jobs:req', job_id, len(set(skills)))
    pipeline.execute()


def find_jobs(conn, candidate_skills):
    skills = {}
    for skill in set(candidate_skills):
        skills['skill:' + skill] = 1

    job_scores = zunion(conn, skills)
    final_result = zintersect(conn, {job_scores: -1, 'job:req': 1})

    return conn.zrangebyscore('idx:' + final_result, 0, 0)


def main():
    conn = redis.StrictRedis(host='localhost', port=6379, db=10, decode_responses=True)
    content = 'in order to construct out SETS of documents, we must first examine out documents for words. ' \
              'the process of extracting words from documents is known as parsing and tokenization;we are ' \
              'producing a set of tokens(or words) that identify the document'
    index_document(conn, 1, content)
    # res = union(conn, ['sets', 'sets'])
    query1 = """
    connect +connection +disconnect +disconnection
    chat
    -proxy -proxies"""
    query = """
    ment +documents +order
    -proxy -proxies"""
    # res = parse(query)
    res = parse_and_search(conn, query)
    print('debug res:', res)


if __name__ == '__main__':
    main()