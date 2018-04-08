package cl.usach.piinf.storm.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import cl.usach.piinf.storm.bolts.IgnoreWordsBolt;
import cl.usach.piinf.storm.bolts.MongoBolt;
import cl.usach.piinf.storm.bolts.UserCounterBolt;
import cl.usach.piinf.storm.bolts.WordCounterBolt;
import cl.usach.piinf.storm.bolts.WordSplitterBolt;
import cl.usach.piinf.storm.spouts.TwitterSampleSpout;

/**
 * Topology class that sets up the Storm topology for this sample.
 */
public class Topology {

	static final String TOPOLOGY_NAME = "storm-twitter-word-count";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
		
		b.setBolt("UserCounterBolt", new UserCounterBolt(10, 1000 * 60, 100)).setNumTasks(1).shuffleGrouping("TwitterSampleSpout");
		
		//b.setBolt("WordSplitterBolt", new WordSplitterBolt(5)).shuffleGrouping("TwitterSampleSpout");
        //b.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt()).shuffleGrouping("WordSplitterBolt");
        //b.setBolt("WordCounterBolt", new WordCounterBolt(10, 100 * 60, 100)).setNumTasks(1).shuffleGrouping("IgnoreWordsBolt");
        
        //b.setBolt("MongoBolt", new MongoBolt()).setNumTasks(1).shuffleGrouping("WordCounterBolt");
        
		if (args != null && args.length > 0) {
			try {
				StormSubmitter.submitTopology(TOPOLOGY_NAME, config, b.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			final LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());
			Utils.sleep(2000000);
			cluster.shutdown();
			
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					cluster.killTopology(TOPOLOGY_NAME);
					cluster.shutdown();
				}
			});
		}
	}
}
