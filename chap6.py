import bisect
import uuid
import time

import redis


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
                pipe.excute()
                return True
            pipe.unwatch()
            break
        except redis.exceptions.WatchError:
            pass

    return False


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
    res = acquire_lock(conn, 'test_setnx')
    print('debug res:', res)


if __name__ == '__main__':
    main()
