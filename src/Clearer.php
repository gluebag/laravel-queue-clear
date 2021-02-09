<?php namespace Morrislaptop\LaravelQueueClear;

use Illuminate\Queue\QueueManager;
use Illuminate\Contracts\Queue\Factory as FactoryContract;
use Morrislaptop\LaravelQueueClear\Contracts\Clearer as ClearerContract;

class Clearer implements ClearerContract
{
    /**
     * @var QueueManager
     */
    protected $manager;

    public $releaseBack = 0;

    /**
     * {@inheritDoc}
     */
    function __construct(FactoryContract $manager)
    {
        $this->manager = $manager;
    }

    /**
     * {@inheritDoc}
     */
    public function clear($connection, $queue)
    {
        $count = 0;
        $connection = $this->manager->connection($connection);

        $count += $this->clearJobs($connection, $queue);
//        $count += $this->clearJobs($connection, $queue . ':reserved');
//        $count += $this->clearDelayedJobs($connection, $queue);

        return $count;
    }

    protected function clearJobs($connection, $queue)
    {
        $count = 0;

        while ($job = $connection->pop($queue)) {

            // If ignorable jobs, just delete
            if(in_array($job->resolveName(), [
                'App\Jobs\SyncRingbaCall',
                'App\Jobs\SyncTrackdriveCall',
            ])) {
                $job->delete();
                $count++;
            }
            else {
                // Requeue it back so it gets processed...
                $job->release();
                $this->releaseBack++;
            }
        }

        return $count;
    }

    protected function clearDelayedJobs($connection, $queue)
    {
        if (method_exists($connection, 'getRedis')) {
            return $this->clearDelayedJobsOnRedis($connection, $queue);
        }

        throw new \InvalidArgumentException('Queue Connection not supported');
    }

    protected function clearDelayedJobsOnRedis($connection, $queue) {
        $key = "queues:{$queue}:delayed";
        $redis = $connection->getRedis()->connection(config('queue.connections.redis.connection'));
        $count = $redis->zcount($key, '-inf', '+inf');
        $redis->del($key);

        return $count;
    }

}
