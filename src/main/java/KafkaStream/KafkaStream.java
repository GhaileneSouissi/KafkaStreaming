package KafkaStream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/***
 * this class is responsible for handling the coming streams
 */
public class KafkaStream {
    /***
     * The main class, it takes a list of strings , the N-1 first elements are topics to consumer and the N topic will be produced to another consumer
     * @param args
     */
    public static void main(String[] args){
        //setting up the application properties
        Properties properties = new Properties();
        PropConfig.setProperties(properties);
        //extracting the arguments
        if (args.length < 2) {
            System.err.println("Please specify at least 2 parameters ");
            System.exit(-1);
        }
        List<String> consumeTopics = new ArrayList<>(Arrays.asList(args)).subList(0,args.length-1);
        String ProduceTopicName = args[args.length-1];

        //the stream builder object , it is responsible for handling the coming streams
        StreamsBuilder builder = new StreamsBuilder();
        //a list of streams to handle different topics(in our case it is one topic)
        List<KStream<String,String>> users = new ArrayList<>();
        for (String topic:consumeTopics){
            users.add(builder.stream(topic, Consumed.with(Topology.AutoOffsetReset.EARLIEST)));
        }

        //genereate a new stream with a new value of ages
        KStream<String,String> newHalfAge = users.get(0).map((key,info)->{
            List<String> list = Arrays.asList(info.split(":"));
            Integer newAge = Integer.parseInt(list.get(1).trim())/2;
            return new KeyValue<>(list.get(0).trim(),newAge.toString());
        });
        //producing a new tream an sending it to another consumer with a topic
        Serde<String> stringSerde = Serdes.String();
        newHalfAge.to(ProduceTopicName,Produced.with(stringSerde,stringSerde));


        //process the coming stream to transfom it into another stream that has the name of the user as a key an the age of the user as a value
        KStream<String,String> maxAgeStream = users.get(0).map((key,info)->{
            List<String> list = Arrays.asList(info.split(":"));
            Integer newAge = Integer.parseInt(list.get(1).trim());
            return new KeyValue<>(list.get(0).trim(),newAge.toString());
        });

        KStream<String,String> maxAge = maxAgeStream.mapValues(v->Long.parseLong(v)).selectKey((k,v)->"c")
                .groupByKey(
                        Serialized.with(
                                Serdes.String(),
                                Serdes.Long())
                ).reduce((a,b)->b>a?b:a).mapValues(v->String.valueOf(v)).toStream();
        maxAge.foreach((name,age)->{
            System.out.println("=================================");
            System.out.println("The maximum age is "+age);
        });

        //process the coming stream to transfom it into another stream that has the name of the user as a key an the age of the user as a value
        KStream<String,String> meanAgeStream = users.get(0).map((key,info)->{
            List<String> list = Arrays.asList(info.split(":"));
            Integer newAge = Integer.parseInt(list.get(1).trim());
            return new KeyValue<>(list.get(0).trim(),newAge.toString());
        });

        //an atomic integer to count the received elements number
        AtomicInteger runCount = new AtomicInteger(0);
        //transform the stream to a new stream that has runCount as a key and the age as a value
        KStream<String,Double> meanAge = meanAgeStream.mapValues(v->Double.parseDouble(v)).selectKey((k,v)->"c")//transforming the key to "c"
                .peek((key, value) ->{//each time an element comes , runcount will be incremented and the value will be printed
                    runCount.incrementAndGet();
                    System.out.println("key=" + runCount + ", value=" + value);
                })
                .groupByKey(//group by "c" , to perform the reduce
                        Serialized.with(
                                Serdes.String(),
                                Serdes.Double())
                ).reduce((a,b)->a+b).toStream();//perform the reduce to get the sum of ages
        //print the average age
        meanAge.foreach((name,age)->{
            System.out.println("=================================");
            System.out.println("The average age is "+age/runCount.intValue());
        });


        //process the coming stream to transfom it into another stream that has the name of the user as a key an the age of the user as a value
        KStream<String,String> minAgeStream = users.get(0).map((key,info)->{
            List<String> list = Arrays.asList(info.split(":"));
            Integer newAge = Integer.parseInt(list.get(1).trim());
            return new KeyValue<>(list.get(0).trim(),newAge.toString());
        });
        //Statefull process to get the min age
        KStream<String,String> minAge = minAgeStream.mapValues(v->Long.parseLong(v)).selectKey((k,v)->"c")//transfoming the key to "c" ,
                .groupByKey(// we will group the elements by this new key to perform the reduce
                        Serialized.with(
                                Serdes.String(),
                                Serdes.Long())
                ).reduce((a,b)->b<a?b:a).mapValues(v->String.valueOf(v)).toStream();
        //print the min age
        minAge.foreach((name,age)->{
            System.out.println("=================================");
            System.out.println("The minimum age is "+age);
        });

        //generate a kafka stream , to start streaming
        KafkaStreams streams = new KafkaStreams(builder.build(),properties);
        //reset the application
        streams.cleanUp();
        //start streaming
        streams.start();
        //stop the streaming
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));
    }
}
