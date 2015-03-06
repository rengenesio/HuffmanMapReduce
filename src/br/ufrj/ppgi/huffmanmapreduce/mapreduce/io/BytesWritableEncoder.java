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

	public int length, bits;
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
		BitUtility.setBit(this.b, this.bits, s);

		if (++this.bits % 8 == 1) {
			this.length++;
		}
	}

	public boolean getBit(int pos) {
		return BitUtility.checkBit(this.b, pos);
	}

	public boolean addBytesWritable(BytesWritableEncoder bw) {
		if (this.b.length < this.length + bw.length) {
			if (!this.setCapacity(this.length + bw.length)) {
				return false;
			}
		}
		
		for (int i = 0; i < bw.bits ; i++) {
			this.addBit(bw.getBit(i));
		}
		
		return true;
	}

	public boolean addCode(Codification c) {
		if (this.b.length < this.length + (c.size / Defines.BYTE_BIT) + 1)
			if(!this.setCapacity(this.length + (c.size / Defines.BYTE_BIT) + 1))
				return false;
				
		for (short i = 0; i < c.size; i++) {
			if (c.code[i] == 0) {
				this.addBit(false);
			}
			else {
				this.addBit(true);
			}
		}
		
		return true;
	}
	
	public void clean() {
		int bitsMod = this.bits % 8;
		
		if(bitsMod != 0) {
			b[0] = b[this.bits / 8];
			this.length = 1;
			this.bits = bitsMod;
		}
		else {
			this.length = 0;
			this.bits = 0;
		}
	}
}
