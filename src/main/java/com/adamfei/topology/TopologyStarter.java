package com.adamfei.topology;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;

import io.latent.storm.rabbitmq.RabbitMQSpout;
import io.latent.storm.rabbitmq.config.ConnectionConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfigBuilder;

import com.adamfei.hbase.mapper.MyHBaseMapper;
import com.adamfei.schema.MyCustomMessageScheme;
import com.adamfei.util.PropertiesLoader;
import com.rabbitmq.client.ConnectionFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.Scheme;

/**
 * storm整个程序入口
 * @author adam
 *
 */
public class TopologyStarter {

	private final static String PROPERTIES_PATH="config.properties";
	
	private final static String RABBIT_HOST_KEY="rabbit.host";
	private final static String RABBIT_PORT_KEY="rabbit.port";
	private final static String RABBIT_USERNAME_KEY="rabbit.username";
	private final static String RABBIT_PASSWORD_KEY="rabbit.password";
	private final static String RABBIT_QUEUE_KEY="rabbit.queue";
	
	private final static String REDIS_HOST_KEY="redis.host";
	private final static String REDIS_PORT_KEY="redis.port";
	
	private final static String HBASE_ROOT_DIR_KEY="hbase.rootdir";
	
	/**
	 * 测试   从rabbit读取数据 直接保存到hbase中
	 * @param args
	 */
	public static void main(String []args){
		
		//--1. 加载配置文件
		Map<String, String> configMap = PropertiesLoader.load(PROPERTIES_PATH);
		
 		
		TopologyBuilder builder = new TopologyBuilder();
		
		
		//--2. 构建rabbit spout
		Scheme scheme = new MyCustomMessageScheme();
		RabbitMQSpout spout = new RabbitMQSpout(scheme);
		
		ConnectionConfig connectionConfig = 
				new ConnectionConfig(
						configMap.get(RABBIT_HOST_KEY), Integer.parseInt(configMap.get(RABBIT_PORT_KEY)),
						configMap.get(RABBIT_USERNAME_KEY), configMap.get(RABBIT_PASSWORD_KEY), 
						ConnectionFactory.DEFAULT_VHOST, 10); // host, port, username, password, virtualHost, heartBeat 
		ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
		                                                        .queue(configMap.get(RABBIT_QUEUE_KEY))
		                                                        .prefetch(200)
		                                                        .requeueOnFail()
		                                                        .build();
		
		builder.setSpout("rabbit-spout", spout).addConfigurations(spoutConfig.asMap()).setMaxSpoutPending(20);
		

	
		
		//builder.setBolt("store-bolt", new StoreRedisBolt(), 1).shuffleGrouping("rabbit-spout");
		
		
		//--3. 构建hbase bolt
		HBaseMapper mapper = new MyHBaseMapper();
		HBaseBolt hbaseBolt = new HBaseBolt("stormTest", mapper).withConfigKey("hbase.conf");
		
		builder.setBolt("store-bolt", hbaseBolt, 1).shuffleGrouping("rabbit-spout");
		
		
		Config conf = new Config();
        conf.setDebug(true);
        
        Map<String, Object> hbConf = new HashMap<String, Object>();
        hbConf.put("hbase.rootdir", configMap.get(HBASE_ROOT_DIR_KEY));
        conf.put("hbase.conf", hbConf);
        
        //conf.put("redisHost", configMap.get(REDIS_HOST_KEY));
		//conf.put("redisPort", configMap.get(REDIS_PORT_KEY));
        
		try {
			StormSubmitter.submitTopology("myFirstStormProject", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
