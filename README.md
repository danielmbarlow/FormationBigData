
## How I set up Hive
```
git clone https://github.com/big-data-europe/docker-hive.git

docker-compose up

docker-compose cp C:\tmp\orderline.txt hive-server:/opt/hive/examples/files/orderline.txt
docker-compose cp C:\tmp\orders.txt hive-server:/opt/hive/examples/files/orders.txt

docker-compose exec hive-server bash

# In the container
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000

# In Hive console
CREATE TABLE order_lines (
    OrderLine INT,
    OrderId INT,
    ProductId INT,
    ShipDate TIMESTAMP,
    BillDate TIMESTAMP,
    UnitPrice DOUBLE,
    NumUnits INT,
    TotalPrice DOUBLE);
    
CREATE TABLE orders (
        OrderId INT,
        CustomerId INT,
        CampaignId INT,
        OrderDate TIMESTAMP,
        City STRING,
        State STRING,
        ZipCode STRING,
        PaymentType STRING,
        TotalPrice DOUBLE,
        NumOrderLines INT,
        NumUnits INT
);

LOAD DATA LOCAL INPATH '/opt/hive/examples/files/orderline.txt' OVERWRITE INTO TABLE order_lines;
# => No rows affected (4.435 seconds)
select count(*) from order_lines;
# => 286018

LOAD DATA LOCAL INPATH '/opt/hive/examples/files/orders.txt' OVERWRITE INTO TABLE orders;
# => No rows affected (1.838 seconds)
select count(*) from orders;
# => 192984
```

