<?php
namespace Octava\Component\Replication\Batch;

class BatchReport
{
    /**
     * @var int
     */
    protected $inserted = 0;

    /**
     * @var int
     */
    protected $updated = 0;

    /**
     * @var int
     */
    protected $skipped = 0;

    public function addInserted($cnt)
    {
        $this->inserted += $cnt;
    }

    public function addUpdated($cnt)
    {
        $this->updated += $cnt;
    }

    public function addSkipped($cnt)
    {
        $this->skipped += $cnt;
    }

    /**
     * @return integer
     */
    public function getInserted()
    {
        return $this->inserted;
    }

    /**
     * @return integer
     */
    public function getUpdated()
    {
        return $this->updated;
    }

    /**
     * @return integer
     */
    public function getSkipped()
    {
        return $this->skipped;
    }


    public function getReportData()
    {
        return [
            'inserted' => $this->inserted,
            'updated' => $this->updated,
            'skipped' => $this->skipped
        ];
    }

    /**
     * Добавить данные из другого обхекта группового отчёта
     * @param BatchReport $batchReport
     * @return $this
     */
    public function addByBatchReport(BatchReport $batchReport)
    {
        $this->addInserted($batchReport->getInserted());
        $this->addUpdated($batchReport->getUpdated());
        $this->addSkipped($batchReport->getSkipped());

        return $this;
    }
}
