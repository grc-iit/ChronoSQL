select * from <log>;
select * from <log> where EID > 1628780806 AND EID < 1658105202;
select count(*) from <log>;
select * from <log> where EID = 'Saturday' OR EID = 'Sunday';
select window(1 day) as one_day, count(*) from <log> group by one_day;
select window(1 month) as one_month, count(*) from <log> group by one_month;
select * from <log> where EID > 1658105202;
select window(1 day) as one_day, count(*) from <log> where EID > 1641936187 and EID < 1652395282 group by one_day;
select window(1 day) as one_day, count(*) from <log> where EID = 'Monday' and EID > 1641936187 and EID < 1652395282 group by one_day;
