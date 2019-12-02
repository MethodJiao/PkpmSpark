package spark.bim.redisConnect

import redis.clients.jedis.Jedis

class RedisConnector {
  def GetRedisConnect(): Jedis = {
    val redisConnect = new Jedis("10.100.140.127", 6379, 3000)
    redisConnect
  }
}
