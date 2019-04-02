package KafkaStream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/***
 * Properties configuration class
 */
public class PropConfig {
    /***
     * setting properties for consumer
     * @param props
     */
    public static void setProperties(Properties props){
        //Application name
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"kafka-stream-application");
        //application broker
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"10.75.17.84:9092");
        // Set how to serialize key/value pairs
        // Set how to deserialize key/value pairs
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());






    }
}