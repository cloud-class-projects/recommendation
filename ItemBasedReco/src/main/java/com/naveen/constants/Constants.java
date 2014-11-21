package com.naveen.constants;

public class Constants
{
  public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
  
  public static final String HBASE_ZOOKEEPER_QUORUM_VALUE = "vmadhire-2.novalocal,vmadhire-3.novalocal,vmadhire-4.novalocal";
  
  public static final String HBASE_ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
  
  public static final String HBASE_ZOOKEEPER_CLIENT_PORT_VALUE = "2181";
  
  public static final String HBASE_USER_TABLE_NAME = "user_rating";
  
  public static final String HBASE_USER_TABLE_CF_NAME = "ratings";
  
  public static final String INPUT_PATH_DATASCRUB = "/user/naveen/Input";
  
  public static final String OUTPUT_PATH_DATASCRUB = "/user/naveen/UserVectors";
  
  public static final String OUTPUT_COOCCURRENCE_MATRIX = "/user/naveen/Co-occurrenceVectors";
  
  public static final String OUTPUT_ITEM_VECTORS = "/user/naveen/ItemVectors";
  
  public static final String OUTPUT_MERGE_COOC_ITEM = "/user/naveen/MegeVector";
  
  public static final String LOG4J_PROPERTIES_FILE = "/home/naveen/log4j.properties";
}