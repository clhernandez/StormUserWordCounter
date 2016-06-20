package cl.usach.piinf.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import cl.usach.piinf.storm.bolts.IgnoreWordsBolt;
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

		b.setBolt("WordSplitterBolt", new WordSplitterBolt(5)).shuffleGrouping("TwitterSampleSpout");
        b.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt()).shuffleGrouping("WordSplitterBolt");
        b.setBolt("WordCounterBolt", new WordCounterBolt(10, 100 * 60, 100)).setNumTasks(1).shuffleGrouping("IgnoreWordsBolt");
        

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});

	}

}
