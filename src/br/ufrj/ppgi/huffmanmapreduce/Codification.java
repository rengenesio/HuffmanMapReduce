package br.ufrj.ppgi.huffmanmapreduce;

public class Codification {
	public byte symbol;
	public byte size;
	public String code;

	public Codification(byte symbol, String code) {
		this.symbol = symbol;
		this.size = (byte) code.length();
		this.code = code;
	}

	public Codification(byte symbol, byte size, String code) {
		this.symbol = symbol;
		this.size = size;
		this.code = code;
	}

	public byte[] toByteArray() {
		byte[] b = new byte[this.size + 2];
		b[0] = this.symbol;
		b[1] = this.size;
		for (int i = 0; i < this.size; i++)
			b[i + 2] = (byte) code.charAt(i);

		return b;
	}

	public char[] toCharArray() {
		char[] c = new char[this.size + 2];
		c[0] = (char) this.symbol;
		c[1] = (char) this.size;

		for (int i = 0; i < this.size; i++)
			c[i + 2] = (char) code.charAt(i);

		return c;
	}

	public String toString() {
		return ((this.symbol & 0xFF) + "(" + this.size + ") " + this.code);
	}
}
