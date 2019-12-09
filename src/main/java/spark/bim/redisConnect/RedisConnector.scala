package spark.bim.redisConnect

import redis.clients.jedis.Jedis

object RedisConnector {
  private val redisConnector = RedisConnector

  def getInstance = redisConnector

  private val redisConnect = new Jedis("10.100.140.9", 6379, 3000)

  def GetRedisConnect(): Jedis = {
    redisConnect
  }
}
