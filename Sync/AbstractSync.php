<?php
namespace Octava\Component\Replication\Sync;

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
     * @return bool
     * @throws \InvalidArgumentException
     */
    protected function existsChanges(array $checkColumns, array $localData, array $remoteData)
    {
        $result = false;
        foreach ($checkColumns as $name => $type) {
            if (!array_key_exists($name, $localData)) {
                throw new \InvalidArgumentException(
                    sprintf('Check column %s does not exists in $localData', $name)
                );
            }
            if (array_key_exists($name, $remoteData) && $localData[$name] != $remoteData[$name]) {
                $this->getLogger()->debug(
                    'Detect change',
                    [
                        'name' => $name,
                        'val1' => $localData[$name],
                        'val2' => $remoteData[$name],
                        'remote' => $remoteData,
                    ]
                );

                $result = true;
                break;
            }
        }
        return $result;
    }

    /**
     * Получить изменившиеся значения полей
     * @param array $checkColumns
     * @param array $localData
     * @param array $remoteData
     * @return array
     * @throws \InvalidArgumentException
     */
    protected function getUpdateValues(array $checkColumns, array $localData, array $remoteData)
    {
        $result = [];
        foreach ($checkColumns as $name => $type) {
            if (array_key_exists($name, $remoteData)) {
                if ((string)$remoteData[$name] !== '' && !array_key_exists($name, $localData)) {
                    $msg = sprintf('Check column %s does not exists in $localData', $name);

                    $this->getLogger()->error($msg, ['remote' => $remoteData, 'local' => $localData]);
                    throw new \InvalidArgumentException($msg);
                }

                if ($localData[$name] != $remoteData[$name]) {
                    $result[$name] = $remoteData[$name];
                }
            }
        }

        return $result;
    }

    /**
     * Получить отчёт по сравнению данных из удалённого источника с локальными
     * @param array $checkColumns
     * @param array $localData
     * @param array $remoteData
     * @return array
     */
    protected function getUpdateReport(array $checkColumns, array $localData, array $remoteData)
    {
        $updateValues = $this->getUpdateValues($checkColumns, $localData, $remoteData);

        $changes = [];
        foreach ($updateValues as $name => $remoteValue) {
            $localValue = $localData[$name];

            if ($localValue instanceof \DateTime) {
                $localValue = $localValue->format('Y-m-d H:i:s');
            }

            if ($remoteValue instanceof \DateTime) {
                $remoteValue = $remoteValue->format('Y-m-d H:i:s');
            }

            $changes[$name] = ['local' => $localValue, 'remote' => $remoteValue];
        }

        return $changes;
    }
}
