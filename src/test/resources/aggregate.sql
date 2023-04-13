select l_linestatus, avg(l_extendedprice), sum(l_orderkey), min(l_quantity), max(l_orderkey), count(l_linestatus) from lineitem 
where l_quantity > 2 and l_returnflag like 'R%' group by l_linestatus;
