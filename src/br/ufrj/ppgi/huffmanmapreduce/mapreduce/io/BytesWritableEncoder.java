package br.ufrj.ppgi.huffmanmapreduce.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;

import br.ufrj.ppgi.huffmanmapreduce.BitUtility;
import br.ufrj.ppgi.huffmanmapreduce.Codification;
import br.ufrj.ppgi.huffmanmapreduce.Defines;

public class BytesWritableEncoder extends BinaryComparable implements WritableComparable<BinaryComparable> {
	private static final byte[] EMPTY_BYTES = {};

	public int index, length, bits;
	public byte[] b;
	public boolean complete = false;

	public BytesWritableEncoder() {
		this(EMPTY_BYTES, 0, 0);
	}

	public BytesWritableEncoder(int capacity) {
		this(EMPTY_BYTES, 0, 0);
		this.setCapacity(capacity);
	}

	public BytesWritableEncoder(byte[] b, int bytes, int bits) {
		this.b = b;
		this.length = bytes;
		this.bits = bits;
		this.index = bits / 8;
	}
	
	@Override
	public byte[] getBytes() {
		return b;
	}

	@Override
	public int getLength() {
		return length;
	}

	public void setSize(int size) {
		if (size > b.length) {
			setCapacity(size * 3 / 2);
		}
		this.length = size;
	}

	public boolean setCapacity(int new_cap) {
		// ERRO ALOCANDO 69175754
		if (new_cap > 67108864) {
			return false;
		}
		else if (new_cap > b.length) {
			byte[] new_data = null;
			try {
				new_data = new byte[new_cap];
			} catch (Exception e) {
				System.out.println("BytesWritableHuffman.setCapacity()\n-----------\nNew size error: " + new_cap);
				e.printStackTrace();
			}
			System.arraycopy(this.b, 0, new_data, 0, this.length);
			this.b = new_data;
		}
		
		return true;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		setSize(0);
		setSize(in.readInt());
		this.bits = in.readInt();
		in.readFully(b, 0, length);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(length);
		out.writeInt(bits);
		out.write(b, 0, length);
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object right_obj) {
		if (right_obj instanceof BytesWritableEncoder)
			return super.equals(right_obj);
		return false;
	}

	@Override
	public String toString() {
		String s = new String();
		for (int i = 0; i < length; i++) {
			s += Integer.toHexString((0xFF & b[i]) >> 4);
			s += Integer.toHexString(0xF & b[i]);
			s += " ";
		}

		s += "--> bits: " + this.bits + "(" + this.length + " bytes)";
		return s;
	}

	public void addBit(boolean s) {
		int pos = Defines.BYTE_BIT - (this.bits % Defines.BYTE_BIT) - 1;

		BitUtility.setBit(this.b, pos, s);

		if (++this.bits % 8 == 1)
			this.length++;
		if (pos == 0)
			this.index++;
	}

	public boolean getBit(int pos) {
		int bit = b[pos / 8] & (1 << Defines.BYTE_BIT - (pos % 8) - 1);
		
		return BitUtility.checkBit(this.b, bit);
	}

	public boolean addBytesWritable(BytesWritableEncoder bw) {
		if (this.b.length < this.getLength() + bw.getLength())
			if (!this.setCapacity(this.getLength() + bw.getLength()))
				return false;
		for (int i = 0; i < bw.bits ; i++)
			this.addBit(bw.getBit(i));
		
		return true;
	}

	public boolean addCode(Codification c) {
		if (this.b.length < this.getLength() + (c.size / 8) + 1)
			if(!this.setCapacity(this.getLength() + (c.size / 8) + 1))
				return false;
				
		for (short i = 0; i < c.size; i++) {
			if (c.code[i] == 0)
				this.addBit(false);
			else
				this.addBit(true);
		}
		
		return true;
	}
	
	public void clean() {
		if(this.index == this.getLength()) {
			this.length = 0;
			this.bits = 0;
			this.index = 0;
		}
		else {
			b[0] = b[index];
			this.length = 1;
			this.bits %= 8;
			this.index = 0;
		}
	}
}
