 #spark-submit --class com.vitessedata.spark.driver.KiteDataSourceRunner --master local[2] target/spark-kite-3.3.2-tests.jar lineitem $HOME/p/spark-kite/src/test/resources/lineitemdec.schema "kite://localhost:7878/test_tpch/csv/lineitem*" $HOME/p/spark-kite/src/test/resources/aggregate.sql
spark-submit --class com.vitessedata.spark.driver.KiteDataSourceRunner --master local[2] target/spark-kite-3.3.2-tests.jar lineitem $HOME/p/spark-kite/src/test/resources/lineitem.schema "kite://localhost:7878/test_tpch/csv/lineitem*" $HOME/p/spark-kite/src/test/resources/aggregate.sql

