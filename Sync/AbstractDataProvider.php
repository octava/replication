<?php
namespace Octava\Component\Replication\Sync;

use Psr\Log\LoggerInterface;

abstract class AbstractDataProvider
{
    /**
     * @var LoggerInterface
     */
    protected $logger;
    /**
     * @var bool
     */
    protected $continueFetch = true;

    /**
     * @param AbstractFilter $filter
     * @param $offset
     * @param $limit
     * @return mixed
     */
    abstract public function fetch(AbstractFilter $filter, $offset, $limit);

    /**
     * @return boolean
     */
    public function isContinueFetch()
    {
        return $this->continueFetch;
    }

    /**
     * @param boolean $continueFetch
     * @return self
     */
    public function setContinueFetch($continueFetch)
    {
        $this->continueFetch = $continueFetch;
        return $this;
    }

    /**
     * @return LoggerInterface
     */
    public function getLogger()
    {
        return $this->logger;
    }

    /**
     * @param LoggerInterface $logger
     * @return self
     */
    public function setLogger($logger)
    {
        $this->logger = $logger;
        return $this;
    }
}
