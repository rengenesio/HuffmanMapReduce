package br.ufrj.ppgi.huffmanmapreduce.tdd;

import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.BytesWritableEncoder;


public class BytesWritableEncoderTests {

	public static boolean test() throws Exception {
		System.out.println("Testando BytesWritableEncoder");
				
		BytesWritableEncoder bytesWritableEncoder = new BytesWritableEncoder(4096);
//		bytesWritableEncoder.addBit(true);
//		bytesWritableEncoder.addBit(false);
//		bytesWritableEncoder.addBit(false);
//		bytesWritableEncoder.addBit(false);
//		bytesWritableEncoder.addBit(false);
//		bytesWritableEncoder.addBit(false);
//		bytesWritableEncoder.addBit(false);
//		bytesWritableEncoder.addBit(false);
//		
//		bytesWritableEncoder.clean();
		

//		bytesWritableEncoder.addBit(true);
//		bytesWritableEncoder.addBit(false);
//		bytesWritableEncoder.addBit(false);
//		bytesWritableEncoder.addBit(false);
//		bytesWritableEncoder.addBit(false);
//		bytesWritableEncoder.addBit(false);
//		bytesWritableEncoder.addBit(false);
//		bytesWritableEncoder.addBit(true);
//		
//		bytesWritableEncoder.addBit(true);

		
		System.out.println(bytesWritableEncoder.toString());
		
//		byteArray[0] = 10; // 00001010
//		byteArray[1] = 38; // 00100110
//		byteArray[2] = 15; // 00001111
//		byteArray[3] = -1; // 11111111
//		byteArray[4] = 98; // 01100010
//		
//		if (BitUtility.checkBit(byteArray, 0) != false) throw new Exception("Erro checando bit! " + 0);
//		if (BitUtility.checkBit(byteArray, 1) != false) throw new Exception("Erro checando bit! " + 1);
//		if (BitUtility.checkBit(byteArray, 2) != false) throw new Exception("Erro checando bit! " + 2);
//		if (BitUtility.checkBit(byteArray, 3) != false) throw new Exception("Erro checando bit! " + 3);
//		if (BitUtility.checkBit(byteArray, 4) != true) throw new Exception("Erro checando bit! " + 4);
//		if (BitUtility.checkBit(byteArray, 5) != false) throw new Exception("Erro checando bit! " + 5);
//		if (BitUtility.checkBit(byteArray, 6) != true) throw new Exception("Erro checando bit! " + 6);
//		if (BitUtility.checkBit(byteArray, 7) != false) throw new Exception("Erro checando bit! " + 7);
//		if (BitUtility.checkBit(byteArray, 8) != false) throw new Exception("Erro checando bit! " + 8);
//		if (BitUtility.checkBit(byteArray, 9) != false) throw new Exception("Erro checando bit! " + 9);
//		if (BitUtility.checkBit(byteArray, 10) != true) throw new Exception("Erro checando bit! " + 10);
//		if (BitUtility.checkBit(byteArray, 11) != false) throw new Exception("Erro checando bit! " + 11);
//		if (BitUtility.checkBit(byteArray, 12) != false) throw new Exception("Erro checando bit! " + 12);
//		if (BitUtility.checkBit(byteArray, 13) != true) throw new Exception("Erro checando bit! " + 13);
//		if (BitUtility.checkBit(byteArray, 14) != true) throw new Exception("Erro checando bit! " + 14);
//		if (BitUtility.checkBit(byteArray, 15) != false) throw new Exception("Erro checando bit! " + 15);
//		if (BitUtility.checkBit(byteArray, 16) != false) throw new Exception("Erro checando bit! " + 16);
//		if (BitUtility.checkBit(byteArray, 17) != false) throw new Exception("Erro checando bit! " + 17);
//		if (BitUtility.checkBit(byteArray, 18) != false) throw new Exception("Erro checando bit! " + 18);
//		if (BitUtility.checkBit(byteArray, 19) != false) throw new Exception("Erro checando bit! " + 19);
//		if (BitUtility.checkBit(byteArray, 20) != true) throw new Exception("Erro checando bit! " + 20);
//		if (BitUtility.checkBit(byteArray, 21) != true) throw new Exception("Erro checando bit! " + 21);
//		if (BitUtility.checkBit(byteArray, 22) != true) throw new Exception("Erro checando bit! " + 22);
//		if (BitUtility.checkBit(byteArray, 23) != true) throw new Exception("Erro checando bit! " + 23);
//		if (BitUtility.checkBit(byteArray, 24) != true) throw new Exception("Erro checando bit! " + 24);
//		if (BitUtility.checkBit(byteArray, 25) != true) throw new Exception("Erro checando bit! " + 25);
//		if (BitUtility.checkBit(byteArray, 26) != true) throw new Exception("Erro checando bit! " + 26);
//		if (BitUtility.checkBit(byteArray, 27) != true) throw new Exception("Erro checando bit! " + 27);
//		if (BitUtility.checkBit(byteArray, 28) != true) throw new Exception("Erro checando bit! " + 28);
//		if (BitUtility.checkBit(byteArray, 29) != true) throw new Exception("Erro checando bit! " + 29);
//		if (BitUtility.checkBit(byteArray, 30) != true) throw new Exception("Erro checando bit! " + 30);
//		if (BitUtility.checkBit(byteArray, 31) != true) throw new Exception("Erro checando bit! " + 31);
//		if (BitUtility.checkBit(byteArray, 32) != false) throw new Exception("Erro checando bit! " + 32);
//		if (BitUtility.checkBit(byteArray, 33) != true) throw new Exception("Erro checando bit! " + 33);
//		if (BitUtility.checkBit(byteArray, 34) != true) throw new Exception("Erro checando bit! " + 34);
//		if (BitUtility.checkBit(byteArray, 35) != false) throw new Exception("Erro checando bit! " + 35);
//		if (BitUtility.checkBit(byteArray, 36) != false) throw new Exception("Erro checando bit! " + 36);
//		if (BitUtility.checkBit(byteArray, 37) != false) throw new Exception("Erro checando bit! " + 37);
//		if (BitUtility.checkBit(byteArray, 38) != true) throw new Exception("Erro checando bit! " + 38);
//		if (BitUtility.checkBit(byteArray, 39) != false) throw new Exception("Erro checando bit! " + 39);
//		
//		return true;
//	}
//	
//	public static boolean setBitTest() throws Exception {
//		System.out.println("Testando BitUtility.setBit()");
//		
//		byte[] byteArray = new byte[5];
//		
//		BitUtility.setBit(byteArray, 0, false);
//		BitUtility.setBit(byteArray, 1, false);
//		BitUtility.setBit(byteArray, 2, false);
//		BitUtility.setBit(byteArray, 3, false);
//		BitUtility.setBit(byteArray, 4, true);
//		BitUtility.setBit(byteArray, 5, false);
//		BitUtility.setBit(byteArray, 6, true);
//		BitUtility.setBit(byteArray, 7, false);
//		BitUtility.setBit(byteArray, 8, false);
//		BitUtility.setBit(byteArray, 9, false);
//		BitUtility.setBit(byteArray, 10, true);
//		BitUtility.setBit(byteArray, 11, false);
//		BitUtility.setBit(byteArray, 12, false);
//		BitUtility.setBit(byteArray, 13, true);
//		BitUtility.setBit(byteArray, 14, true);
//		BitUtility.setBit(byteArray, 15, false);
//		BitUtility.setBit(byteArray, 16, false);
//		BitUtility.setBit(byteArray, 17, false);
//		BitUtility.setBit(byteArray, 18, false);
//		BitUtility.setBit(byteArray, 19, false);
//		BitUtility.setBit(byteArray, 20, true);
//		BitUtility.setBit(byteArray, 21, true);
//		BitUtility.setBit(byteArray, 22, true);
//		BitUtility.setBit(byteArray, 23, true);
//		BitUtility.setBit(byteArray, 24, true);
//		BitUtility.setBit(byteArray, 25, true);
//		BitUtility.setBit(byteArray, 26, true);
//		BitUtility.setBit(byteArray, 27, true);
//		BitUtility.setBit(byteArray, 28, true);
//		BitUtility.setBit(byteArray, 29, true);
//		BitUtility.setBit(byteArray, 30, true);
//		BitUtility.setBit(byteArray, 31, true);
//		BitUtility.setBit(byteArray, 32, false);
//		BitUtility.setBit(byteArray, 33, true);
//		BitUtility.setBit(byteArray, 34, true);
//		BitUtility.setBit(byteArray, 35, false);
//		BitUtility.setBit(byteArray, 36, false);
//		BitUtility.setBit(byteArray, 37, false);
//		BitUtility.setBit(byteArray, 38, true);
//		BitUtility.setBit(byteArray, 39, false);	
//		
//		if(byteArray[0] != 10) return false; // 00001010
//		if(byteArray[1] != 38) return false; // 00100110
//		if(byteArray[2] != 15) return false; // 00001111
//		if(byteArray[3] != -1) return false; // 11111111
//		if(byteArray[4] != 98) return false; // 01100010
		
		return true;
	}

}
