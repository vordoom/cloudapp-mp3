
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.esotericsoftware.minlog.Log;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  private BufferedReader fileReader = null;
  private String filePath = null;

  public FileReaderSpout(String filePath){
    this.filePath = filePath;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.context = context;
    this._collector = collector;

    try {
      this.fileReader = new BufferedReader(new FileReader(this.filePath));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);

    try {
      String line = this.fileReader.readLine();
      this._collector.emit(new Values(line));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
    try {
      this.fileReader.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
