# elasticsearch-monthly-roller
In a streaming event system,we keep updating docs to the existing docs. As splitting the index by month, 
we should merge the docs from index to index(eg: move existing docs from customer-201901  to  customer-201812)

将历史时间戳存储在 .kibana 索引。 

例如： 在mesos中做每10分钟计划任务，自动检查index-201901中最新导入的数据，并检查是否index-201812中已存在。
存在则执行迁移动作， 并从新index中删除。
