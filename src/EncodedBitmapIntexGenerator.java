import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EncodedBitmapIntexGenerator {
	private Hashtable<String, Map<Integer, Long>> data_dictionary; // encodedBitmap;
	private int currentRecordCount;
	private int currentBlock;

	private final byte blockLength = Long.SIZE;

	public EncodedBitmapIntexGenerator() {
		data_dictionary = new Hashtable<>();
		// encodedBitmap = new ArrayList<Long>();
	}

	public boolean insert(String recordvalue) {
		Map<Integer, Long> encodedBitmap = data_dictionary.get(recordvalue);
		if (encodedBitmap == null) {
			encodedBitmap = new HashMap<>();
		}
		int blockIndex = currentRecordCount - currentBlock * (blockLength - 1);
		if (blockIndex == blockLength - 1) {
			blockIndex = 0;
			currentBlock++;
		}
		long encodedNumber = 0L;
		if (blockIndex != 0) {
			if (encodedBitmap.size() <= currentBlock) {
				for (int k = currentBlock - 1; k >= 0; k--) {
					if (encodedBitmap.get(k) == null) {
						encodedBitmap.put(k, 0L);
					}
				}
				encodedBitmap.put(currentBlock, 0L);
			}
			encodedNumber = encodedBitmap.get(currentBlock);
		}
		long num = (long) Math.pow(2, blockIndex);
		encodedNumber = encodedNumber + (long) Math.pow(2, blockIndex);

		
		// System.out.println(recordvalue + ":\t"
		// + Long.toBinaryString(encodedNumber) + "\tencoded:"
		// + encodedNumber + "\tnum:" + num + "\trecord:"
		// + currentRecordCount + "\t:" + blockIndex + "\t:"
		// + currentBlock);
		for (int k = currentBlock - 1; k >= 0; k--) {
			if (encodedBitmap.get(k) == null) {
				encodedBitmap.put(k, 0L);
			}
		}
		encodedBitmap.put(currentBlock, encodedNumber);
		data_dictionary.put(recordvalue, encodedBitmap);
		currentRecordCount++;
		return false;
	}

	public List<Integer> getIdArray(String recordValue) {
		List<Integer> ans = new LinkedList<>();
		Map<Integer, Long> encodedBitmap = data_dictionary.get(recordValue);
		int index = 0;
		for (int block = 0; block < currentBlock; block++) {
			long encodedVal = encodedBitmap.get(block);
			int blockIndex = 0;
			long num = (long) Math.pow(2, blockLength - 1 - blockIndex);
			while (encodedVal != 0) {

				if (encodedVal % 2 == 1) {
					ans.add(index);
				}
				index++;
				blockIndex++;
			}
		}
		return ans;
	}

	public void print(String recordValue) {
		Map<Integer, Long> encodedBitmap = data_dictionary.get(recordValue);
		if (encodedBitmap != null) {
			for (int block = 0; block <= currentBlock; block++) {
				long encodedVal = encodedBitmap.get(block);
				System.out.print(Long.toBinaryString(encodedVal)+"|");
			}
			System.out.println();
		} else {
			System.out.println("Key not found:" + recordValue);
		}
	}

	public static void main(String[] args) {
		EncodedBitmapIntexGenerator e = new EncodedBitmapIntexGenerator();
		for (int i = 0; i < 1000; i++) {
			e.insert("ABC" + i % 3);
		}
		e.print("ABC0");
	}

}
