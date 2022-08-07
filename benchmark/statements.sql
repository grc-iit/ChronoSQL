select *
from <log>;
select *
from <log>
where EID > 1628780806 AND EID < 1658105202;
select *
from <log>
where EID = 'Saturday' OR EID = 'Sunday';
select window(1 day) as one_day, count(*)
from <log>
group by one_day;
select *
from <log>
where EID > 1658105202;
