package com.kofiska.solana.orchestrator.infra.valkey

import com.kofiska.solana.orchestrator.ports.DedupeCache
import io.lettuce.core.SetArgs
import io.lettuce.core.codec.StringCodec
import io.lettuce.core.{KeyScanCursor, RedisClient, RedisURI, ScanArgs, ScanCursor}

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters._

final class ValkeyDedupeCache(redisUri: String)(implicit ec: ExecutionContext) extends DedupeCache with AutoCloseable {
  private val client = RedisClient.create(RedisURI.create(redisUri))
  client.setDefaultTimeout(Duration.ofSeconds(5))
  @volatile private var connection = client.connect(StringCodec.UTF8)
  @volatile private var commands = connection.sync()

  override def get(requestId: String): Future[Option[String]] =
    Future(blocking { Option(execute(_.get(key(requestId)))) })

  override def claim(requestId: String, marker: String, ttlSeconds: Long): Future[Boolean] =
    Future(blocking { execute(_.set(key(requestId), marker, SetArgs.Builder.nx().ex(ttlSeconds))) != null })

  override def put(requestId: String, decisionId: String, ttlSeconds: Long): Future[Unit] =
    Future(blocking { execute(_.setex(key(requestId), ttlSeconds, decisionId)); () })

  override def delete(requestId: String): Future[Unit] =
    Future(blocking { execute(_.del(key(requestId))); () })

  override def scan(prefix: String, limit: Int): Future[Vector[String]] =
    Future(blocking {
      execute { commands =>
        val builder = Vector.newBuilder[String]
        var count = 0
        var cursor: ScanCursor = ScanCursor.INITIAL
        val args = ScanArgs.Builder.matches(s"$prefix*").limit(math.max(1, math.min(limit, 1000)))
        do {
          val scanned: KeyScanCursor[String] = commands.scan(cursor, args)
          cursor = scanned
          val remaining = limit - count
          val keys = scanned.getKeys.asScala.take(remaining).toVector
          builder ++= keys
          count += keys.size
        } while (!cursor.isFinished && count < limit)
        builder.result()
      }
    })

  private def key(requestId: String): String =
    s"solana:dedupe:$requestId"

  override def close(): Unit = {
    connection.close()
    client.close()
  }

  private def execute[A](operation: io.lettuce.core.api.sync.RedisCommands[String, String] => A): A =
    try {
      operation(commands)
    } catch {
      case error: Throwable =>
        refresh()
        operation(commands)
    }

  private def refresh(): Unit = synchronized {
    try connection.close()
    catch { case _: Throwable => () }
    connection = client.connect(StringCodec.UTF8)
    commands = connection.sync()
  }
}
