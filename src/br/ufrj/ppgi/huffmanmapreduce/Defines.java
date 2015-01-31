package br.ufrj.ppgi.huffmanmapreduce;

public class Defines {
	//public static final String HDFS_SERVER = "hdfs://localhost:9000";
	//public static final String HDFS_SERVER = "hdfs://hadoop-nn:9000";
	
	public static final int POWER_BITS_CODIFICATION = 256;
	
	public static final int BYTE_BIT = 8;
	
	
	public static final int EOB = 3;
	public static final int EOF = 4;
	
	
	/*
	#define BYTE								byte
	#define FREQUENCY							int / long

	#define SYMBOL								byte
	#define SIZE								short // Imediatamente maior que SYMBOL
	#define POWER_SIZE							int // Imediatamente maior que SIZE
	#define FILE_SIZE							int // Tamanho máximo do arquivo
	#define BITS_CODIFICATION					8
	#define BYTE_BIT							8
	#define SIZEOF_SYMBOL						1
	#define POWER_BITS_CODIFICATION				256
	#define ALIGN_NODE							2
	#define ALIGN_NODE_ARRAY					6
	#define ALIGN_TREE_ARRAY					6
	#define ALIGN_STACK							6
	#define ALIGN_CODIFICATION					5
	#define ALIGN_CODIFICATION_ARRAY_ELEMENT	6
	*/
}
