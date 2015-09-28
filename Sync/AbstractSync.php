<?php
namespace Octava\Component\Replication\Sync;

use Monolog\Handler\NullHandler;
use Octava\Component\Replication\Batch\BatchReport;
use Octava\Component\Replication\Batch\BatchSaver;
use Symfony\Bridge\Monolog\Logger;

abstract class AbstractSync
{
    /**
     * @var int
     */
    protected $limit;

    /**
     * @var Logger
     */
    protected $logger;

    /**
     * @var AbstractDataProvider[]
     */
    protected $dataProviders = [];

    /**
     * @var AbstractFilter
     */
    protected $filter;

    /**
     * @var BatchSaver
     */
    protected $batchSaver;

    /**
     * @var BatchReport
     */
    protected $batchReport;

    /**
     * @var array
     */
    protected $availableAliases = [];

    public function __construct(BatchSaver $batchSaver, $limit = 500)
    {
        $this->setBatchSaver($batchSaver);
        $this->setBatchReport(new BatchReport());

        $this->limit = $limit;
    }

    /**
     * @return int
     */
    public function getLimit()
    {
        return $this->limit;
    }

    /**
     * @param int $limit
     * @return self
     */
    public function setLimit($limit)
    {
        $this->limit = $limit;
        return $this;
    }

    /**
     * @return BatchReport
     */
    public function getBatchReport()
    {
        return $this->batchReport;
    }

    /**
     * @param BatchReport $batchReport
     * @return self
     */
    public function setBatchReport($batchReport)
    {
        $this->batchReport = $batchReport;
        return $this;
    }

    abstract public function sync(array $rows);

    /**
     * @return BatchSaver
     */
    public function getBatchSaver()
    {
        return $this->batchSaver;
    }

    /**
     * @param BatchSaver $batchSaver
     * @return AbstractSync
     */
    public function setBatchSaver($batchSaver)
    {
        $this->batchSaver = $batchSaver;
        return $this;
    }

    /**
     * @return AbstractFilter
     */
    public function getFilter()
    {
        return $this->filter;
    }

    /**
     * @param AbstractFilter $filter
     * @return AbstractSync
     */
    public function setFilter($filter)
    {
        $this->filter = $filter;
        return $this;
    }

    public function run(AbstractFilter $filter)
    {
        $this->setFilter($filter);
        $this->getBatchSaver()->setLogger($this->getLogger());

        $this->walk([$this, 'sync'], $this->getLimit());

        $this->getLogger()->info('Total report', [$this->getBatchReport()->getReportData()]);
    }

    /**
     * @return Logger
     */
    public function getLogger()
    {
        if (null === $this->logger) {
            $this->logger = new Logger(__CLASS__);
            $this->logger->pushHandler(new NullHandler());
        }
        return $this->logger;
    }

    /**
     * @param Logger $logger
     * @return AbstractSync
     */
    public function setLogger($logger)
    {
        $this->logger = $logger;
        return $this;
    }

    public function addDataProvider(AbstractDataProvider $dataProvider)
    {
        $this->dataProviders[] = $dataProvider;
        return $this;
    }

    /**
     * @return AbstractDataProvider[]
     */
    public function getDataProviders()
    {
        return $this->dataProviders;
    }

    /**
     * @param AbstractDataProvider[] $dataProviders
     */
    public function setDataProviders(array $dataProviders)
    {
        $this->dataProviders = $dataProviders;
    }

    /**
     * @return array
     */
    public function getAvailableAliases()
    {
        return $this->availableAliases;
    }

    /**
     * @param array $availableAliases
     * @return AbstractSync
     */
    public function setAvailableAliases($availableAliases)
    {
        $this->availableAliases = $availableAliases;
        return $this;
    }

    public function isDifferent($local, $remote)
    {
        $flag = false;
        if ($local instanceof \DateTime) {
            $local = $local->format('Y-m-d H:i:s');
        }
        if ($remote instanceof \DateTime) {
            $remote = $remote->format('Y-m-d H:i:s');
        }

        if (is_numeric($local) && is_numeric($remote)) {
            $flag = 0 !== bccomp($local, $remote, 50);
        } elseif ($local != $remote) {
            $flag = true;
        }
        return $flag;
    }

    protected function walk(callable $callback, $limit = 500)
    {
        $this->getLogger()->info(sprintf('Initialized %d data provider(s)', count($this->getDataProviders())));

        $dataProviders = $this->getDataProviders();
        $provider = $this->shiftProvider($dataProviders);

        if ($provider) {
            $offset = 0;
            do {
                $rows = $provider->fetch($this->getFilter(), $offset, $limit);
                if ($provider) {
                    $this->getLogger()->debug(
                        sprintf('Fetch provider %s, rows %d', get_class($provider), count($rows))
                    );
                }
                if (!$provider->isContinueFetch() && count($dataProviders) > 0) {
                    $provider = $this->shiftProvider($dataProviders);
                    $offset = 0;
                } else {
                    $offset += $limit;
                }
                if ($rows) {
                    call_user_func($callback, $rows);
                }
            } while ($provider !== null && $provider->isContinueFetch());
        }
    }

    /**
     * @param AbstractDataProvider[] $dataProviders
     * @return AbstractDataProvider
     */
    protected function shiftProvider(array &$dataProviders)
    {
        if (count($dataProviders) === 0) {
            return null;
        }
        $result = array_shift($dataProviders);
        $result->setLogger($this->getLogger());
        return $result;
    }

    /**
     * Проверить есть ли изменения в данных из удалённого источника относительно локальных данных
     * @param array $checkColumns
     * @param array $localData
     * @param array $remoteData
     * @param array $changes
     * @return bool
     */
    protected function existsChanges(array $checkColumns, array $localData, array $remoteData, array &$changes = null)
    {
        $data = $this->getUpdateValues($checkColumns, $localData, $remoteData, $changes);

        $result = false;
        if (!empty($data)) {
            $result = true;

            foreach ($changes as $name => $change) {
                $this->getLogger()->debug(
                    'Detect change',
                    [
                        'name' => $name,
                        'val1' => $change['local'],
                        'val2' => $change['remote'],
                        'remote' => $remoteData,
                    ]
                );
            }
        }
        return $result;
    }

    /**
     * Получить изменившиеся значения полей
     * @param array $checkColumns
     * @param array $localData
     * @param array $remoteData
     * @param array $report
     * @return array
     */
    protected function getUpdateValues(array $checkColumns, array $localData, array $remoteData, array &$report = null)
    {
        $result = [];
        foreach ($checkColumns as $name => $type) {
            if (!array_key_exists($name, $localData)) {
                throw new \InvalidArgumentException(
                    sprintf('Check column %s does not exists in $localData', $name)
                );
            }
            if (array_key_exists($name, $remoteData)) {
                $local = $localData[$name];
                $remote = $remoteData[$name];
                $isDifferent = $this->isDifferent($local, $remote);

                if ($isDifferent) {
                    $result[$name] = $remote;
                    $report[$name] = ['local' => $local, 'remote' => $remote];
                }
            }
        }
        return $result;
    }
}
