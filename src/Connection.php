<?php

namespace Soyuka\RedisMessengerAdapter;

use Soyuka\Adapter\PredisAdapter;

class Connection
{
    const PROCESSING_QUEUE_SUFFIX = '_processing';

    private $connection;

    public function __construct($url = '127.0.0.1', $port = 6379, $password = null, $sentinel_hosts = null, $serializer = \Redis::SERIALIZER_PHP)
    {
        if ($sentinel_hosts && is_array($sentinel_hosts)) {
            list($url, $port) = $this->sentinelConnection($sentinel_hosts);
        }

        $this->connection = new \Redis();
        $this->connection->connect($url, $port);
        (!is_null($password) ? $this->connection->auth($password) : null);
        $this->connection->setOption(\Redis::OPT_SERIALIZER, $serializer);
        $this->connection->setOption(\Redis::OPT_READ_TIMEOUT, -1);
    }

    // @TODO
    public static function fromDsn(string $dsn): self
    {
        return new self();
    }

    public function sentinelConnection($sentinel_hosts)
    {
        foreach ($sentinel_hosts as $sentinel_host) {
            $redis = new \Redis();
            if ($redis->connect($sentinel_host, '26379')) {
                $master = $redis->rawCommand('sentinel', 'master', 'mymaster');
                $master = $this->parseArrayResult($master);
                return [$master['ip'], $master['port']];
            }
        }

        return false;
    }

    private function parseArrayResult(array $data)
    {
        $result = array();
        $count = count($data);
        for ($i = 0; $i < $count;) {
            $record = $data[$i];
            if (is_array($record)) {
                $result[] = $this->parseArrayResult($record);
                $i++;
            } else {
                $result[$record] = $data[$i + 1];
                $i += 2;
            }
        }
        return $result;
    }
    /**
     * Takes last element (tail) of the list and add it to the processing queue (head - blocking)
     * Also sets a key with TTL that will be checked by the `doCheck` method.
     */
    public function waitAndGet(string $queue, int $processingTtl = 10000, int $blockingTimeout = 1000): ?array
    {
        $this->doCheck($queue);
        $value = $this->connection->bRPopLPush($queue, $queue . self::PROCESSING_QUEUE_SUFFIX, $blockingTimeout);

        // false in case of timeout
        if (false === $value) {
            return null;
        }

        $key = md5($value['body']);
        $this->connection->set($key, 1, array('px' => $processingTtl));

        return $value;
    }

    /**
     * Acknowledge the message:
     * 1. Remove the ttl key
     * 2. LREM the message from the processing list.
     */
    public function ack(string $queue, $message)
    {
        $key = md5($message['body']);
        $processingQueue = $queue . self::PROCESSING_QUEUE_SUFFIX;
        $transaction = $this->connection->multi();
        $transaction->lRem($processingQueue, $message)->del($key)->exec();
    }

    /**
     * Reject message, means we add it back to the queue
     * All we have to do is to make our key expire and let the `doCheck` system manage it.
     */
    public function reject(string $queue, $message)
    {
        $key = md5($message['body']);
        $this->connection->expire($key, -1);
    }

    /**
     * Add item at the tail of list.
     */
    public function add(string $queue, $message)
    {
        $this->connection->rpush($queue, $message);
    }

    /**
     * The check:
     * 1. Get the processing queue items
     * 2. Check if the TTL is over
     * 3. If it is, rpush back the message to the origin queue.
     */
    private function doCheck(string $queue)
    {
        $processingQueue = $queue . self::PROCESSING_QUEUE_SUFFIX;
        $pending = $this->connection->lRange($processingQueue, 0, -1);

        foreach ($pending as $temp) {
            $key = md5($temp['body']);

            if ($this->connection->ttl($key) > 0) {
                continue;
            }

            $transaction = $this->connection
                ->multi()
                ->del($key)
                ->lRem($processingQueue, $temp, 1)
                ->rPush($queue, $temp)
                ->exec();
        }
    }
}
