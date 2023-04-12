select l_linestatus, avg(l_extendedprice), sum(l_orderkey), min(l_quantity), max(l_orderkey), count(l_linestatus) from lineitem 
where l_quantity > 2 group by l_linestatus;
