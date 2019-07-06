import time
import bisect
import logging
from datetime import datetime
import functools
import redis
import json


# 使用redis来记录最新日志

SEVERITY = {
    logging.DEBUG: 'debug',
    logging.INFO: 'info',
    logging.WARNING: 'warning',
    logging.ERROR: 'error',
    logging.CRITICAL: 'critical'
}
# python3中字典的values()方法返回的类型是dict_values，要将其转换为列表
SEVERITY.update((name, name) for name in list(SEVERITY.values()))
SEVERITY = {
    10: 'debug',
    20: 'info',
    30: 'warning',
    40: 'error',
    50: 'critical',
    'debug': 'debug',
    'info': 'info',
    'warning': 'warning',
    'error': 'error',
    'critical': 'critical'
}


def log_recent(conn, name, message, severity=logging.INFO, pipe=None):
    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = 'recent:{}:{}'.format(name, severity)
    message = time.asctime() + ' ' + message
    # or的用法非常巧妙
    pipe = pipe or conn.pipeline()
    # 用一个list来记录日志信息
    pipe.lpush(destination, message)
    pipe.ltrim(destination, 0, 9)
    pipe.execute()
    return destination


# 记住特定消息出现的频率，并根据出现频率的高低来决定消息的排放顺序，从而找出最重要的信息
def log_common(conn, name, message, severity=logging.INFO, timeout=5):
    # import pdb
    # pdb.set_trace()
    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = 'common:{}:{}'.format(name, severity)
    start_key = destination + ':start'
    pipe = conn.pipeline()
    end = time.time() + timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)
            now = datetime.utcnow().timetuple()
            hour_start = datetime(*now[:4]).isoformat()
            existing = pipe.get(start_key)
            print('debug start_key:', start_key)
            print('debug hour_start, existing:', hour_start, existing)
            pipe.multi()
            # 处理上一小时搜集到的日志
            if existing and existing < hour_start:
                print('existing')
                pipe.rename(destination, destination + ':last')
                pipe.rename(start_key, destination + ':pstart')
                pipe.set(start_key, hour_start)
            # 如果redis中没有保存过common_log,设置common_log开始时间为当前小时的时间
            elif not existing:
                print('not existing')
                pipe.set(start_key, hour_start)
            pipe.zincrby(destination, 1, message)
            log_recent(pipe, name, message, severity, pipe)
            return
        except redis.exceptions.WatchError:
            continue






PRECISION = [1, 5, 60, 300, 3600, 18000, 86400]


def update_counter(conn, name, count=1, now=None):
    now = now or time.time()
    pipe = conn.pipeline()
    for prec in PRECISION:
        pnow = int(now / prec) * prec

        hash = '{}:{}'.format(prec, name)
        # 注意python3中zadd的写法
        pipe.zadd('known:', {hash: 0})
        pipe.hincrby('count:' + hash, pnow, count)
    pipe.execute()


def get_counter(conn, name, precision):
    hash = '{}:{}'.format(precision, name)
    data = conn.hgetall('count:' + hash)
    to_return = []
    for key, value in data.items():
        to_return.append((int(key), int(value)))
    to_return.sort()

    return to_return


QUIT = False
SAMPLE_COUNT = 30


"""
计数器：(hash)
count:1:hits
"1560569260": 1
"1560569920": 2
"1560569925": 2
"1560569930": 1
"1560570115": 1

count:5:hits
"1560569260": 1
"1560569920": 2
"1560569925": 2
"1560569930": 1
"1560570115": 1
...

对被使用的计数器进行记录 (zset)
known:
"18000:hits": 0
"1:hits": 0
"300:hits": 0
"3600:hits": 0
"5:hits": 0
"60:hits": 0
"86400:hits": 0
"""


def clean_counters(conn):
    pipe = conn.pipeline(True)
    passes = 0
    while not QUIT:
        start = time.time()
        index = 0
        # 循环对zset中的计数器进行遍历
        while index < conn.zcard('known:'):
            import pdb
            pdb.set_trace()
            # 取出对应位置上的计数器 ["18000:hits"]
            hash = conn.zrange('known:', index, index)
            index += 1
            if not hash:
                break
            hash = hash[0]
            # hash = 18000:hits
            prec = int(hash.partition(':')[0])
            # prec = 18000
            bprec = int(prec // 60) or 1
            # 清理程序每60秒循环一次，根据计数器的更新频率来判断是否有必要对计数器进行清理
            # 以index = 0为例，该计数器的更新频率为5小时，所以就没必要进行清理
            # bprec = 300
            if passes % bprec:
                continue
            hkey = 'count:' + hash
            # hkey = 'count:18000:hits'
            cutoff = time.time() - SAMPLE_COUNT * prec
            # python3中map返回的是迭代器
            samples = list(map(int, conn.hkeys(hkey)))
            samples.sort()
            # remove是指列表截止元素的index
            remove = bisect.bisect_right(samples, cutoff)
            if remove:
                conn.hdel(hkey, *samples[:remove])
                if remove == len(samples):
                    try:
                        pipe.watch(hkey)
                        if not pipe.hlen(hkey):
                            pipe.multi()
                            pipe.zrem('knonwn:', hash)
                            pipe.execute()
                            index -= 1
                        else:
                            pipe.unwatch()
                    except redis.exceptions.WatchError:
                        pass
        passes += 1
        duration = min(int(time.time() - start) + 1, 60)
        time.sleep(max(60 - duration, 1))


CONFIGS = {}
CHECKED = {}
config_connection = None


def get_config(conn, type, component, wait=1):
    return '{}'
    # key = 'config:%s:%s'%(type, component)
    #
    # # 检查是否需要对这个组件的配置信息进行更新。
    # if CHECKED.get(key) < time.time() - wait:
    #     # 有需要对配置进行更新，记录最后一次检查这个连接的时间。
    #     CHECKED[key] = time.time()
    #     # 取得Redis存储的组件配置。
    #     config = json.loads(conn.get(key) or '{}')
    #     # 将潜在的Unicode关键字参数转换为字符串关键字参数。
    #     config = dict((str(k), config[k]) for k in config)
    #     # 取得组件正在使用的配置。
    #     old_config = CONFIGS.get(key)
    #
    #     # 如果两个配置并不相同……
    #     if config != old_config:
    #         # ……那么对组件的配置进行更新。
    #         CONFIGS[key] = config
    #
    # return CONFIGS.get(key)


REDIS_CONNECTIONS = {}


def redis_connection(component, wait=1):
    key = 'config:redis' + component
    def wrapper(function):
        @functools.wraps(function)
        def call(*args, **kwargs):
            print('debug decorator call function:', function)
            print('debug decorator call args:', *args)
            print('debug decorator call args:', **kwargs)
            # old_config = CONFIGS.get(key, object())
            # _config = get_config(config_connection, 'redis', component, wait)
            #
            config = {}
            # for k, v in _config.iteritems():
            #     config[k.encode('utf-8')] = v
            #
            # if config != old_config:
            REDIS_CONNECTIONS[key] = redis.Redis(**config)
            res = function(REDIS_CONNECTIONS.get(key), *args, **kwargs)
            print('debug res:', res)
            return res
        return call
    return wrapper


@redis_connection('logs')                   # redis_connection()装饰器非常容易使用。
def log_recent(conn, app, message):         # 这个函数的定义和之前展示的一样，没有发生任何变化。
    print('debug conn', conn)
    print('debug app', app)
    print('debug message', message)
    return 'aaa'


def main():
    """
    将计数器存储到redis里面
    需求：
    构建一个能够持续创建并维护计数器的工具
    :return:
    """
    # len_res = []
    conn = redis.StrictRedis(host='localhost', port=6379, db=10, decode_responses=True)
    # name = 'hits'
    # update_counter(conn, name)
    # counter = get_counter(conn, name, 300)
    # len_res.append(len(counter))
    # print('debug counter:', counter)
    # print('res:', len_res)
    # clean_counters(conn)
    # res = []
    # for i in range(30):
    #     des = log_recent(conn, 'common_log', 'common hhhh{}'.format(i))
    #     res.append(des)
    # log_common(conn, 'common_log', 'common hhhh')
    log_recent('main', 'User235 logged in')


if __name__ == '__main__':
    main()