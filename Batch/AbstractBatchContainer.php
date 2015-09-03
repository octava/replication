<?php
namespace Octava\Component\Replication\Batch;

abstract class AbstractBatchContainer
{
    /**
     * @var array
     */
    protected $insertRows = [];
    /**
     * @var array
     */
    protected $updateRows = [];

    public function add(array $item, $id = null)
    {
        if ($id) {
            $this->updateRows[$id] = $item;
        } else {
            $this->insertRows[] = $item;
        }
        return $this;
    }

    /**
     * @return array
     */
    public function getInsertRows()
    {
        return $this->insertRows;
    }

    /**
     * @param array $insertRows
     * @return self
     */
    public function setInsertRows($insertRows)
    {
        $this->insertRows = $insertRows;
        return $this;
    }

    /**
     * @return array
     */
    public function getUpdateRows()
    {
        return $this->updateRows;
    }

    /**
     * @param array $updateRows
     * @return self
     */
    public function setUpdateRows($updateRows)
    {
        $this->updateRows = $updateRows;
        return $this;
    }

    /**
     * @return string
     * @example
     * <code>
     * return 'R12nProjectBundle:Account';
     * </code>
     */
    abstract public function getClassName();

    /**
     * @return array [ ColumnName => ColumnType, ... ]
     * @example
     * <code>
     * return [
     *  'createdAt' => \PDO::PARAM_STR,
     *  'updatedAt' => \PDO::PARAM_STR,
     *  'originalId' => \PDO::PARAM_INT,
     *  'clientOriginalId' => \PDO::PARAM_INT,
     *  'accountNumber' => \PDO::PARAM_STR,
     *  'projectId' => \PDO::PARAM_INT,
     * ];
     * </code>
     */
    abstract public function getInsertColumns();

    /**
     * @return array [ ColumnName => ColumnType, ... ]
     * @example
     * <code>
     * $result = $this->getInsertColumns();
     * unset($result['createdAt']);
     * return $result;
     * </code>
     */
    abstract public function getUpdateColumns();
}
