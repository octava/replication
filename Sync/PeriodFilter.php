<?php
namespace Octava\Component\Replication\Sync;

class PeriodFilter extends AbstractFilter
{
    /** @var \DateTime */
    protected $from;
    /** @var \DateTime */
    protected $to;

    /** @return \DateTime */
    public function getFrom()
    {
        return $this->from;
    }

    /** @return \DateTime */
    public function getTo()
    {
        return $this->to;
    }
}
