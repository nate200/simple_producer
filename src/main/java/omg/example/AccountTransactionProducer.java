package omg.example;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;

import omg.example.model.Account;
import static omg.example.BasicPropFactory.genDefaultProdProp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class AccountTransactionProducer {
    private static final Logger log = LoggerFactory.getLogger(AccountTransactionProducer.class.getName());
    /*
    ./kafka-topics.sh --bootstrap-server :9092 --create --topic account-point-transaction \
        --partitions 3 \
        --replication-factor 3 \
        --config min.insync.replicas=2
    */
    public static void main(String[] args) throws InterruptedException {
        Account[] accounts = getAccounts();
        int accLen = accounts.length;

        int sendMsgCount = randomInt(10000, 20000);
        final String TOPIC = "account-point-transaction";
        KafkaProducer<String, Integer> producer = createNewKafkaProducer();
        for(int i = 0; i < sendMsgCount; i++){
            Account account = accounts[randomInt(0, accLen)];
            int pointChanged = randomInt(-50, 100);

            account.changePoint(pointChanged);

            ProducerRecord<String,Integer> record = new ProducerRecord<>(TOPIC, account.name(), pointChanged);
            log.info("sending a record#" + i + " : " + record.key() + ":" +record.value());
            producer.send(record);

            /*RecordMetadata recordMeta = producer.send(record).get();
            handleResult(recordMeta, null);*/

            /*producer.send(record, (recordMetadata, e) -> {
                handleResult(recordMetadata, e);
            });
            //producer.send(record, AccountTransactionProducer::handleResult);*/

            simulateLag(i);
        }

        log.info("final result: ");
        for(Account acc : accounts)
            log.info(acc.toString());
        log.info("sendMsgCount: " + sendMsgCount);

        producer.flush();
        producer.close();
    }

    private static void simulateLag(int sendMsgCount) throws InterruptedException {
        if(sendMsgCount % 1423 == 0 || sendMsgCount % 3173 == 0)
            Thread.sleep(randomInt(50, 100));//lag main thread
    }
    private static Account[] getAccounts(){
        Account[] accounts = new Account[]{
            new Account("Sophie Gonzalez"),
            new Account("Tyler Schultz"),
            new Account("Gail Steele"),
            new Account("Edward Santiago"),
            new Account("Erica White"),
            new Account("Fred Payne"),
            new Account("Gwen Thomas"),
            new Account("Tamara Shelton"),
            new Account("Sarah Little"),
            new Account("Flora Summers")
        };

        log.info("final result: ");
        for(Account acc : accounts)
            log.info(acc.toString());

        return accounts;
    }
    private static KafkaProducer<String, Integer> createNewKafkaProducer(){
        Map<String,Object> prop = genDefaultProdProp();
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        prop.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        /*prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        prop.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);*/
        return new KafkaProducer<>(prop);
    }
    private static void handleResult(RecordMetadata recordMetadata, Exception e){
        if(e != null)
            log.info("Unexpected error: " + e.getMessage());
        else
            log.info("Successfully received the details as: \n" +
                "Topic:" + recordMetadata.topic() + "\n" +
                "Partition:" + recordMetadata.partition() + "\n" +
                "Offset" + recordMetadata.offset() + "\n" +
                "Timestamp" + recordMetadata.timestamp()
            );
    }
    private static int randomInt(int s, int e){
        return ThreadLocalRandom.current().nextInt(s,e);
    }
}
