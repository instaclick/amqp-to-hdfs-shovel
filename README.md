amqp-to-hdfs-shovel
===================


Building
--------
The amqp-to-hdfs-shovel is built with Apache Maven. 
All you'll need to get started is maven.

    $ git clone git@github.com:instaclick/amqp-to-hdfs-shovel.git
    $ cd amqp-to-hdfs-shovel
    $ mvn clean package
    $ java -Dlog4j.configuration=file:target/install/etc/amqp-to-hdfs-shovel/log4j.properties \
        -jar target/install/opt/amqp-to-hdfs-shovel/amqp-to-hdfs-shovel.jar \
        target/install/etc/amqp-to-hdfs-shovel/app.conf
