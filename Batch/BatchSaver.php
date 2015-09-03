<?php
namespace Octava\Component\Replication\Batch;

use Doctrine\DBAL\DBALException;
use Doctrine\ORM\EntityManager;
use Monolog\Handler\NullHandler;
use Symfony\Bridge\Monolog\Logger;

class BatchSaver
{
    /**
     * @var EntityManager
     */
    protected $entityManager;

    /**
     * @var Logger
     */
    protected $logger;

    /**
     * @var int
     */
    protected $batchSize = 100;

    /**
     * @var int
     */
    protected $numberOfRetries = 1;

    /**
     * Generate and run update queries or skip
     * @var bool
     */
    protected $update = true;

    public function __construct(EntityManager $entityManager)
    {
        $this->setEntityManager($entityManager);
    }

    /**
     * @return boolean
     */
    public function isUpdate()
    {
        return $this->update;
    }

    /**
     * @param boolean $update
     * @return self
     */
    public function setUpdate($update)
    {
        $this->update = $update;
        return $this;
    }

    /**
     * @return EntityManager
     */
    public function getEntityManager()
    {
        return $this->entityManager;
    }

    /**
     * @param EntityManager $entityManager
     * @return self
     */
    public function setEntityManager($entityManager)
    {
        $this->entityManager = $entityManager;
        return $this;
    }

    /**
     * @return Logger
     */
    public function getLogger()
    {
        if (!$this->logger) {
            $this->logger = new Logger('batch_save', [new NullHandler()]);
        }
        return $this->logger;
    }

    /**
     * @param Logger $logger
     * @return self
     */
    public function setLogger($logger)
    {
        $this->logger = $logger;
        return $this;
    }

    /**
     * @return int
     */
    public function getBatchSize()
    {
        return $this->batchSize;
    }

    /**
     * @param int $batchSize
     * @return self
     */
    public function setBatchSize($batchSize)
    {
        $this->batchSize = $batchSize;
        return $this;
    }

    /**
     * Save data
     *
     * @param AbstractBatchContainer $batchContainer
     * @param BatchReport $batchReport
     */
    public function save(AbstractBatchContainer $batchContainer, BatchReport $batchReport)
    {
        $this->saveInsert($batchContainer, $batchReport);
        $this->saveUpdate($batchContainer, $batchReport);
    }

    public function saveInsert(AbstractBatchContainer $batchContainer, BatchReport $batchReport)
    {
        $result = 0;
        $rows = $batchContainer->getInsertRows();
        if ($rows) {
            $i = 1;
            $batchCnt = 0;
            $countRows = count($rows);

            $totalBatchCnt = ceil($countRows / $this->getBatchSize());
            $this->getLogger()->info(
                'Begin insert actions batch',
                ['rows' => $countRows, 'batchSize' => $this->getBatchSize()]
            );
            $values = [];
            foreach ($rows as $row) {
                $values[] = $this->generateInsertValues($batchContainer, $row);

                if (($i++ % $this->getBatchSize()) === 0) {
                    $query = $this->generateInsertHeader($batchContainer) . "\n" . implode(",\n", $values) . ';';
                    $result += $this->exec($query);
                    $values = [];

                    $batchCnt++;
                    $this->getLogger()->info(sprintf('Complete %d/%d batch', $batchCnt, $totalBatchCnt));
                }
            }
            if (!empty($values)) {
                $query = $this->generateInsertHeader($batchContainer) . "\n" . implode(",\n", $values) . ';';
                $result += $this->exec($query);

                $batchCnt++;
                $this->getLogger()->info(sprintf('Complete %d/%d batch', $batchCnt, $totalBatchCnt));
            }
        }

        $batchReport->addInserted($result);
        return $result;
    }

    public function saveUpdate(AbstractBatchContainer $batchContainer, BatchReport $batchReport)
    {
        $result = 0;
        $rows = $batchContainer->getUpdateRows();
        if ($rows) {
            $i = 1;
            $batchCnt = 0;
            $cnt = count($rows);
            if ($this->isUpdate()) {
                $totalBatchCnt = ceil($cnt / $this->getBatchSize());
                $this->getLogger()->info(
                    'Begin update actions batch',
                    ['rows' => $cnt, 'batchSize' => $this->getBatchSize()]
                );
                $updateQuery = [];
                foreach ($rows as $id => $row) {
                    $updateQuery[] = $this->generateUpdateQuery($batchContainer, $id, $row);

                    if (($i++ % $this->getBatchSize()) === 0) {
                        $query = implode(";\n", $updateQuery);
                        $result += $this->exec($query);
                        $updateQuery = [];

                        $batchCnt++;
                        $this->getLogger()->info(sprintf('Complete %d/%d batch', $batchCnt, $totalBatchCnt));
                    }

                    $batchReport->addUpdated(1);
                }
                if (!empty($updateQuery)) {
                    $query = implode(";\n", $updateQuery);
                    $result += $this->exec($query);

                    $batchCnt++;
                    $this->getLogger()->info(sprintf('Complete %d/%d batch', $batchCnt, $totalBatchCnt));
                }
            } else {
                $batchReport->addSkipped($result);
                $result = 0;
            }
        }
        return $result;
    }

    public function exec($query)
    {
        $connection = $this->getEntityManager()->getConnection();

        $cnt = 0;
        $continue = false;

        $result = null;
        do {
            $connection->beginTransaction();
            try {
                $cnt++;

                $this->getLogger()
                    ->debug('Try exec query', ['query' => $query]);

                $result = $connection->exec($query);
                $connection->commit();

                $continue = false;
            } catch (DBALException $e) {
                $connection->rollBack();

                if ($cnt < $this->getNumberOfRetries()) {
                    $continue = true;
                }

                $this->getLogger()->critical('Exec error', ['message' => $e->getMessage()]);
            }
        } while ($continue);

        return $result;
    }

    /**
     * @return int
     */
    public function getNumberOfRetries()
    {
        return $this->numberOfRetries;
    }

    /**
     * @param int $numberOfRetries
     * @return self
     */
    public function setNumberOfRetries($numberOfRetries)
    {
        $this->numberOfRetries = $numberOfRetries;
        return $this;
    }

    public function generateInsertHeader(AbstractBatchContainer $batchContainer)
    {
        $tableName = $this->getTableName($batchContainer);
        return "INSERT INTO {$tableName} (" . implode(', ', array_keys($batchContainer->getInsertColumns())) . ") VALUES ";
    }

    public function generateInsertValues(AbstractBatchContainer $batchContainer, array $item)
    {
        $result = [];
        foreach ($batchContainer->getInsertColumns() as $columnName => $columnType) {
            $data = array_key_exists($columnName, $item) ? $item[$columnName] : null;
            $result[] = $this->quote($data, $columnType);
        }
        return '(' . implode(', ', $result) . ')';
    }

    public function generateUpdateQuery(AbstractBatchContainer $batchContainer, $id, array $item)
    {
        $tableName = $this->getTableName($batchContainer);
        $sql = "UPDATE {$tableName} SET";
        $values = [];
        foreach ($batchContainer->getUpdateColumns() as $columnName => $columnType) {
            if (array_key_exists($columnName, $item)) {
                $values[] = $columnName . ' = ' . $this->quote($item[$columnName], $columnType);
            }
        }
        $result = sprintf("%s %s WHERE id = %s", $sql, implode(', ', $values), $this->quote($id, \PDO::PARAM_INT));
        return $result;
    }

    public function quote($value, $type)
    {
        if ($value instanceof \DateTime) {
            $value = $value->format('Y-m-d H:i:s');
        }

        $connection = $this->getEntityManager()->getConnection();

        if (is_null($value)) {
            $result = 'NULL';
        } elseif (is_bool($value)) {
            $result = $value ? 1 : 0;
        } else {
            $result = $connection->quote($value, $type);
        }
        return $result;
    }

    protected function getTableName(AbstractBatchContainer $batchContainer)
    {
        return $this->getEntityManager()
            ->getClassMetadata($batchContainer->getClassName())->getTableName();
    }
}
