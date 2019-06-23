import bisect
import uuid
import time
import math
import json
import os
from collections import defaultdict, deque

import redis

QUIT = False


valid_characters = '`abcdefghijklmnopqrstuvwxyz{'

def find_prefix_range(prefix):
    posn = bisect.bisect_left(valid_characters, prefix[-1:])
    suffix = valid_characters[(posn or 1) - 1]
    return prefix[:-1] + suffix + '{', prefix + '{'


def autocomplete_on_prefix(conn, guild, prefix):
    """
    根据前缀自动补全联系人信息，并在集合中挑选最近输入的10条供用户选择
    该解决方案中非常巧妙的一点在于，通过项有序集合添加元素来创建查找范围，
    并在取得范围内的元素之后，移除之前添加的元素
    :param conn:
    :param guild:
    :param prefix:
    :return:
    """
    start, end = find_prefix_range(prefix)
    identifier = str(uuid.uuid4())
    # 为了防止多个程序同时向一个集合中发送消息的时候，将多个相同的起始元素和结束元素重复地添加到有序集合里面
    # 或者错误地从有序集合里面移除了由其他自动补全程序添加的起始元素和结束元素，这里生成一个uuid添加到起始元素和结束元素的后面
    start += identifier
    end += identifier
    zset_name = 'members:' + guild
    conn.zadd(zset_name, 0, start, 0, end)
    pipeline = conn.pipeline(True)
    while 1:
        try:
            pipeline.watch(zset_name)
            sindex = pipeline.zrank(zset_name, start)
            eindex = pipeline.zrank(zset_name, end)
            # 由于range函数取的值左右两边都是闭区间，从start_index开始
            # 如果集合中符合前缀条件的元素多于10个，那么eindex - 2 > sindex + 9, 最多取10个的话，到sindex+9为止
            # 如果集合中符合前缀条件的元素少于10个，那么eindex - 2 < sindex + 9, 最多取10个的话，取不了sindex + 9那么多，因此取eindex - 2
            # 综合两种情况考虑，应该取min(sindex + 9, eindex - 2)
            erange = min(sindex + 9, eindex - 2)
            pipeline.multi()
            # 取得范围之后，将添加的标识元素移除
            pipeline.zrem(zset_name, start, end)
            pipeline.zrange(zset_name, sindex, erange)
            items = pipeline.execute()[-1]
            break
        except redis.exceptions.WatchError:
            continue

    return [item for item in items if '{' not in item]


# 使用redis构建锁
def acquire_lock(conn, lockname, acquire_timeout=10):
    """
    为了拿到某个特定的锁，需要使用setnx将锁的值设置为一个随机数
    只有当之前没有这个锁的时候（即首次对这个锁进行设置值的时候），setnx才能执行成功
    如果有一个客户端已经对其设置了值，那么setnx将执行不成功，acquire_lock失败
    充分利用了setnx()函数的特性
    :param conn:
    :param lockname:
    :param acquire_timeout:
    :return:
    """
    identifier = str(uuid.uuid4())
    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx('lock:' + lockname, identifier):
            return identifier
        time.sleep(0.001)
    return False


# 有了锁之后就可以用来代替watch操作了
def purchase_item_with_lock(conn, buyerid, itemid, sellerid):
    """
    该函数用于模拟商品购买操作
    市场上
    itemA.4 表示商品id和卖家id
    卖家将商品拿到市场上销售
    首先要监视卖家的包裹
    然后将被出售的商品添加到市场上
    最后从卖家的包裹里面移除被出售的商品

    商品的购物过程：
    监视市场以及买家的个人信息，确认买家的钱数和商品的售价
    然后验证买家是否又足够的钱来购买商品
    最后将买家支付的钱转移给卖家
    将商品添加到买家的包裹
    从市场上移除被售出的商品
    :param conn:
    :param buyerid:
    :param itemid:
    :param sellerid:
    :return:
    """
    buyer = 'users:{}'.format(buyerid)
    seller = 'users:{}'.format(sellerid)
    item = '{}.{}'.format(itemid, sellerid)
    inventory = 'inventory:{}'.format(buyerid)

    locked = acquire_lock(conn, 'market')
    if not locked:
        return False
    pipe = conn.pipeline(True)
    try:
        pipe.zscore('market:', item)
        pipe.hget(buyer, 'funds')
        price, funds = pipe.excute()
        if price is None or price > funds:
            return None
        pipe.hincrby(seller, 'funds', int(price))
        pipe.hincrby(buyer, 'funds', int(-price))
        pipe.sadd(inventory, itemid)
        pipe.zrem('market:', item)
        pipe.excute()
        return True
    finally:
        # 最后无论如何都要释放锁
        release_lock(conn, 'market', locked)


def release_lock(conn, lockname, identifier):
    """
    释放锁的操作很简单，将这个锁的键值对删除即可
    需要注意的是，对这个键的操作要确保是线程安全的
    首先要对锁这个键进行监视
    然后要确保键的值没有被修改过
    最后要使用事物来对其进行删除
    :param conn:
    :param lockname:
    :param identifier:
    :return:
    """
    pipe = conn.pipeline(True)
    lockname = 'lock:' + lockname
    while True:
        try:
            pipe.watch(lockname)
            if pipe.get(lockname) == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True
            pipe.unwatch()
            break
        except redis.exceptions.WatchError:
            pass

    return False


def acquire_lock_with_timeout(conn, lockname, acquire_timeout=10, lock_timeout=10):
    """
    加锁是为了防止锁的持有者崩溃的时候，锁不会自动被释放，导致锁一直出于已被获取的状态
    :param conn:
    :param lockname:
    :param acquire_timeout:
    :param lock_timeout:
    :return:
    """
    identifier = uuid.uuid4()
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))

    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx(lockname, identifier):
            # 为锁设置过期时间，使得redis可以自动删除过期的锁
            conn.expire(lockname, lock_timeout)
            return identifier
        # 计算锁的剩余生存时间
        elif not conn.ttl(lockname):
            # 为未设置超时时间的锁设置超时时间
            conn.expire(lockname, lock_timeout)
        time.sleep(0.001)

    return False


def acquire_semaphore(conn, semname, limit, timeout=10):
    """
    计数信号量：可以让用户限制一项资源最多能够同时被多少个进程访问，
    通常用于限定能够同时使用的资源数量
    :param conn:
    :param semname:
    :param limit:
    :param timeout:
    :return:
    """
    identifier = str(uuid.uuid4())
    now = time.time()

    pipeline = conn.pipeline(True)
    # 删除超时时间（10s）以外的所有信号量
    pipeline.zremrangebyscore(semname, '-inf', now - timeout)
    pipeline.zadd(semname, identifier)
    # 获取新增的信号量的排名
    pipeline.zrank(semname, identifier)
    if pipeline.execute()[-1] < limit:
        return identifier
    conn.zrem(semname, identifier)
    return None

'''
任务队列：通过将待执行的任务放入队列里面，并在之后对队列进行处理，
用户可以推迟执行哪些需要一段时间才能完成的工作
这种将工作交给任务处理器来执行对做法被成为任务队列
'''
# 构建一个邮件队列
def send_sold_email_via_queue(conn, seller, item, price, buyer):
    """
    将待发送邮件放入一个由列表结构表示的队列里面
    :param conn:
    :param seller:
    :param item:
    :param price:
    :param buyer:
    :return:
    """
    data = {
        'seller_id': seller,
        'item_id': item,
        'price': price,
        'buyer_id': buyer,
        'time': time.time()
    }
    print('debug data:', json.dumps(data))
    conn.rpush('queue:email', json.dumps(data))


def process_sold_email_queue(conn):
    """
    由于使用阻塞氏弹出，一次只能从队列里面弹出一封待发送邮件
    希望一个队列能够处理多种不同类型的任务
    :param conn:
    :return:
    """
    while not QUIT:
        # 使用阻塞版本的lpop() 从对列中弹出 一封 待发送的邮件，
        # 如果队列中存在数据，返回；不存在则在等待30s之后返回
        packed = conn.blpop(['queue:email'], 30)
        print('debug packed:', packed)
        if not packed:
            continue
        to_send = json.loads(packed[1])
        # try:
        #     fetch_data_and_send_sold_email(to_send)
        # except EmailSendError as err:
        #     pass
        # else:
        #     log_success('send sold email')

def foo(*args):
    print('callback args:', args)


def worker_watch_queue(conn, queue, callbacks):
    """
    把邮件发送程序写成回调
    :param conn:
    :param queue:
    :param callbacks:
    :return:
    """
    global QUIT
    while not QUIT:
        packed = conn.blpop([queue], 30)
        if not packed:
            continue
        name, *args = json.loads(packed[1])
        if name not in callbacks:
            print('unknown callback {}'.format(name))
            continue
        callbacks[name](*args)
        QUIT = False


def worker_watch_queues(conn, queues, callbacks):
    """
    用多个队列来实现优先级队列
    :return:
    """
    while not QUIT:
        packed = conn.blpop(queues)
        if not packed:
            continue
        name, args = json.loads(packed[1])
        if name not in callbacks:
            print('unknown callback')
            continue
        callbacks[name](*args)


def execute_later(conn, queue, name, args, delay=0):
    """
    延迟任务
    首先，把所有需要在未来执行的任务都添加到有序集合里面，并将任务的执行时间设置为分值
    这样就能够确保取出来的任务永远是当前最先需要执行的
    然后，用一个进程来查找有序集合里面是否存在可以立即被执行的任务
    有的话，就从有序集合里面移除那个任务，并将它添加到适当的任务队列里面
    :param conn:
    :param queue:
    :param name:
    :param args:
    :param delay:
    :return:
    """
    identifier = str(uuid.uuid4())
    # 每个任务用一个json列表来表示
    # 唯一标示符，处理任务的队列的名字，处理任务的回调函数的名字，传递给回调函数的参数
    item = json.dumps([identifier, queue, name, args])
    if delay > 0:
        conn.zadd('delayed:', time.time() + delay, item)
    else:
        # 立即可执行的任务将被直接插在任务队列里面
        conn.rpush('queue:' + queue, item)
    return identifier


def poll_queue(conn):
    """
    从延迟队列里面获取可执行任务
    :param conn:
    :return:
    """
    global QUIT
    while not QUIT:
        item = conn.zrange('delayed:', 0, 0, withscores=True)
        # 延迟队列里面没有包含任何任务或者任务的执行时间未到
        if not item or item[0][1] > time.time():
            time.sleep(0.01)
            continue
        item = item[0][0]
        identifier, queue, func, args = json.loads(item)
        locked = acquire_lock(conn, identifier)
        if not locked:
            continue
        if conn.zrem('delayed:', item):
            conn.rpush('queue:' + queue, item)

        release_lock(conn, identifier, locked)


def create_chat(conn, sender, recipients, message, chat_id=None):
    """
    创建群组聊天会话
    :param conn:
    :param sender:
    :param recipients:
    :param message:
    :param chat_id:
    :return:
    """
    chat_id = chat_id or str(conn.incr('ids:chat:'))
    recipients.append(sender)
    # 利用生成器表达式生成字典
    recipientsd = dict((r, 0) for r in recipients)
    pipeline = conn.pipeline(True)
    pipeline.zadd('chat:' + chat_id, **recipientsd)
    for rec in recipients:
        pipeline.zadd('seen:' + rec, 0, chat_id)
    pipeline.execute()

    return send_message(conn, chat_id, sender, message)


def send_message(conn, chat_id, sender, message):
    """
    发送消息
    :param conn:
    :param chat_id:
    :param sender:
    :param message:
    :return:
    """
    identifier = acquire_lock(conn, 'chat:' + chat_id)
    if not identifier:
        raise Exception('cannot get the lock')
    try:
        mid = conn.incr('ids:' + chat_id)
        ts = time.time()
        packed = json.dumps({
            'id': mid,
            'ts': ts,
            'sender': sender,
            'message': message
        })
        conn.zadd('msgs:' + chat_id, mid, packed)
    finally:
        release_lock(conn, 'chat:' + chat_id, identifier)

    return chat_id


def fetch_pending_message(conn, recipient):
    """
    获取消息
    :param conn:
    :param recipient:
    :return:
    """
    # import pdb
    # pdb.set_trace()
    seen = conn.zrange('seen:' + recipient, 0, -1, withscores=True)
    pipeline = conn.pipeline(True)
    for chat_id, seen_id in seen:
        pipeline.zrangebyscore('msgs:' + chat_id, seen_id + 1, 'inf')
    chat_info = zip(seen, pipeline.execute())
    for i, ((chat_id, seen_id), messages) in enumerate(chat_info):
        print('debug i, chat_id, seen_id, messages:', i, chat_id, seen_id, messages)
        if not messages:
            continue
        messages[:] = map(json.loads, messages)
        seen_id = messages[-1]['id']
        conn.zadd('chat:' + chat_id, seen_id, recipient)

        min_id = conn.zrange('chat:' + chat_id, 0, 0, withscores=True)
        print('debug min_id:', min_id)
        pipeline.zadd('seen:' + recipient, chat_id, seen_id)
        if min_id:
            pipeline.zrangebyscore('msgs:' + chat_id, 0, min_id[0][1])
        chat_info = dict(chat_info)
        chat_info[i] = (chat_id, messages)
    pipeline.execute()

    return chat_info


aggregates = defaultdict(lambda: defaultdict(int))

def daily_country_aggregates(conn, line):
    if line:
        line = line.split()
        ip = line[0]
        day = line[1]
        country = find_city_by_ip_local(ip)
        aggregates[day][country] += 1
    for day, aggregate in list(aggregates.items()):
        conn.zadd('daily:country:' + day, **aggregate)
        del aggregates[day]


def find_city_by_ip_local(ip):
    return 'USA'


def copy_logs_to_redis(conn, path, channel, count=10, limit=2**30, quit_when_done=True):
    """
    复制日志文件并在之后对无用的数据进行清理
    :param conn:
    :param path:
    :param channel:
    :param count:
    :param limit:
    :param quit_when_done:
    :return:
    """
    bytes_in_redis = 0
    waiting = deque()
    # 创建用于向客户端发送消息的群组
    create_chat(conn, 'source', list(map(str, range(count))), '', channel)
    count = str(count)
    # 遍历所有日志文件
    for logfile in sorted(os.listdir(path)):
        full_path = os.path.join(path, logfile)
        fsize = os.stat(full_path).st_size
        while bytes_in_redis + fsize > limit:
            cleaned = _clean(conn, channel, waiting, count)
            if cleaned:
                bytes_in_redis -= cleaned
            else:
                time.sleep(0.25)

        with open(full_path, 'rb') as inp:
            block = ''
            while block:
                block = inp.read(2 ** 17)
                conn.append(channel + logfile, block)

        # 提醒监听者，文件已经准备就绪
        send_message(conn, channel, 'source', logfile)

        bytes_in_redis += fsize
        waiting.append((logfile, fsize))

    if quit_when_done:
        send_message(conn, channel, 'source', ':done')

    while waiting:
        cleaned = _clean(conn, channel, waiting, count)
        if cleaned:
            bytes_in_redis -= cleaned
        else:
            time.sleep(0.25)


def _clean(conn, channel, waiting, count):
    if not waiting:
        return 0
    w0 = waiting[0][0]
    if conn.get(channel + w0 + ':done') == count:
        conn.delete(channel + w0, channel + w0 + ':done')
        return waiting.popleft()[1]
    return 0


def process_logs_from_redis(conn, id, callback):
    while 1:
        fdata = fetch_pending_message(conn, id)
        for ch, mdata in fdata:
            for message in mdata:
                logfile = message['message']
                if logfile == ':done':
                    return
                elif not logfile:
                    continue

                block_reader = readblocks
                for line in readlines(conn, ch+logfile, block_reader):
                    callback(conn, line)
                callback(conn, None)
                conn.incr(ch + logfile + ':done')
        if not fdata:
            time.sleep(0.1)


def readblocks(conn, key, blocksize=2**17):
    """
    从redis里面读取数据块
    :param conn:
    :param key:
    :param blocksize:
    :return:
    """
    lb = blocksize
    pos = 0
    while lb == blocksize:
        block = conn.substr(key, pos, pos + blocksize - 1)
        yield block
        lb = len(block)
        pos += lb
    yield ''


def readlines(conn, key, rblocks):
    out = ''
    for block in rblocks:
        out += block
        posn = out.rfind('\n')
        if posn > 0:
            for line in out[:posn].split('\n'):
                yield line + '\n'
            out = out[posn+1:]
        if not block:
            yield out
            break


def main():
    """
    将计数器存储到redis里面
    需求：
    构建一个能够持续创建并维护计数器的工具
    :return:
    """
    conn = redis.StrictRedis(host='localhost', port=6379, db=10, decode_responses=True)
    # res = autocomplete_on_prefix(conn, 'learn', 'abc')
    # print('debug res:', res)
    # res = acquire_lock(conn, 'test_setnx')
    # print('debug res:', res)
    # send_sold_email_via_queue(conn, 'he', 'book', 200, 'qiao')
    # process_sold_email_queue(conn)

    # conn.rpush('queue:email', json.dumps(('foo', [1, 2, 3])))
    # worker_watch_queue(conn, 'queue:email', {'foo': foo})
    # execute_later(conn, 'test_delay', 'foo', [1, 2, 3], delay=3)
    # poll_queue(conn)

    # create_chat(conn, 'he2', ['xuke', 'dazhi'], 'python group')
    # send_message(conn, '1', 'he2', 'the second message')
    #
    # chat_info = fetch_pending_message(conn, 'he2')
    # print('debug chat_info:', chat_info)

    line = '173.194.38.137 2011-19-10 13:55:36 achievement-762'
    daily_country_aggregates(conn, line)


if __name__ == '__main__':
    main()
