import bisect
import uuid
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


def main():
    """
    将计数器存储到redis里面
    需求：
    构建一个能够持续创建并维护计数器的工具
    :return:
    """
    conn = redis.StrictRedis(host='localhost', port=6379, db=10, decode_responses=True)
    res = autocomplete_on_prefix(conn, 'learn', 'abc')
    print('debug res:', res)


if __name__ == '__main__':
    main()
