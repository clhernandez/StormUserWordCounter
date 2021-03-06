package cl.usach.piinf.storm.bolts;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;



/**
 * Bolt filters out a predefined set of words.
 */
public class IgnoreWordsBolt extends BaseRichBolt {

    private Set<String> IGNORE_LIST = new HashSet<String>(Arrays.asList(new String[] {
    		"♥","¡","!","?","¿","a","ante","bajo","cabe","con","contra","de","desde","en","entre","hacia","hasta","para","por","segun","sin","so","sobre","tras","el","la","los","las","un","una","unos","unas","uno","dos","tres","cuatro","cinco","seis","siete","ocho","nueve","cero","tambien","y","pero","si","bien","ahora","bien","por", "porque","por que","lo","cual","asi","vaya","alla","ahi","bien","que","con","choro", "es","que","no","te","fue","ese","me","https","ja"
    }));
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String lang = (String) input.getValueByField("lang");
        String word = (String) input.getValueByField("word");
        
        if (!IGNORE_LIST.contains(word) && lang.equals("es")) {
            collector.emit(new Values(lang, word));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "word"));
    }
}
