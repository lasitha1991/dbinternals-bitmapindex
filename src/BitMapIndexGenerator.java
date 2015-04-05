import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

/**
 * BitMapIndexGenerator generates bitmap index for a given data field (one
 * attribute) and stores it in the memory. Assumption id = row_number
 */
public class BitMapIndexGenerator {
	private static final int INSTRUCTION_BIT_LENGTH = 64;
	private static final int FIRST_ONE_BIT_LENGTH = 16;
	private static final int ONE_BITCOUNT_LENGTH = 24;

	private static final int BIN_COUNT = 25;

	private static final String DELETE_BITMAP_INDEX_NAME = "DeletedBitMapString";
	private static final String FIRST_ONE_BIT_UPDATE_COMMAND = "FirstOneBit";
	private static final String ONE_BITCOUNT_UPDATE_COMMAND = "OneBitLength";
	/**
	 * Global variables for index.
	 * */
	int initial_length_of_bit_index;
	int expanding_index_size;
	int bitindex_info_len;
	int no_of_diff_values;
	int current_record_count;

	Hashtable<String, boolean[]> data_dictionary;
	ArrayList<String> datacoloumn;

	/*
	 * variables to keep in the front of every index no_of_records in the bit
	 * string no_of_records in the whole database
	 */
	public BitMapIndexGenerator(int initial_length_of_bit_index,
			int expanding_index_size) {
		this.initial_length_of_bit_index = initial_length_of_bit_index;
		this.expanding_index_size = expanding_index_size;
		this.data_dictionary = new Hashtable<String, boolean[]>();
	}

	public BitMapIndexGenerator(ArrayList<String> datacolumn,
			int initial_length_of_bit_index, int expanding_index_size) {
		this.datacoloumn = datacolumn;
		this.initial_length_of_bit_index = initial_length_of_bit_index;
		this.expanding_index_size = expanding_index_size;
		this.data_dictionary = new Hashtable<String, boolean[]>();
		this.CreateBitMapIndex();
	}

	/**
	 * Update information will update the informations bit sector. 64 bits long
	 * currently 1) 15 bits for the first 1 ==> thus if the first 1 bit is more
	 * than 1024 records away consider compressing. 2) 32 bits for the number of
	 * 1 ==> number of true records
	 * **/
	private void UpdateFirstBit(int firstBitIndex, String recordValue) {

		try {
			boolean[] boolean_bitString = data_dictionary.get(recordValue);
			if (firstBitIndex >= 0) {
				// An Insert occured.
				String bitString = Integer.toBinaryString(firstBitIndex);
				if (bitString.length() > FIRST_ONE_BIT_LENGTH) {
					// Compress the bit string and rearrange the bit string
				} else {
					int counter = FIRST_ONE_BIT_LENGTH - bitString.length();
					for (char bit : bitString.toCharArray()) {
						if (bit == '1') {
							boolean_bitString[counter] = true;
						} else {
							boolean_bitString[counter] = false;
						}
						counter++;
					}
				}
			} else {
				// Case 1: An delete occurred.
				// Case 2: An update has entered a new record the bitmap. Have
				// to check
				// Whether this row is before the current first bit
				firstBitIndex = -1 * firstBitIndex;
				char[] bin_firstBitIndex = new char[FIRST_ONE_BIT_LENGTH];
				for (int counter = 0; counter < FIRST_ONE_BIT_LENGTH; counter++) {
					if (boolean_bitString[counter] == true) {
						bin_firstBitIndex[counter] = '1';
					} else {
						bin_firstBitIndex[counter] = '0';
					}
				}
				int current_firstBitIndex = Integer.parseInt(
						String.valueOf(bin_firstBitIndex), 2);
				if (current_firstBitIndex == firstBitIndex) {
					// Case 1
					for (int counter = FIRST_ONE_BIT_LENGTH
							+ ONE_BITCOUNT_LENGTH + current_firstBitIndex + 1; counter < boolean_bitString.length; counter++) {
						if (boolean_bitString[counter] == true) {
							current_firstBitIndex = counter
									- (FIRST_ONE_BIT_LENGTH
											+ ONE_BITCOUNT_LENGTH + 1);
							break;
						}
					}
					bin_firstBitIndex = Integer.toBinaryString(
							current_firstBitIndex).toCharArray();
					for (int counter = 0; counter < FIRST_ONE_BIT_LENGTH; counter++) {
						if (bin_firstBitIndex[counter] == '1') {
							boolean_bitString[counter] = true;
						} else {
							boolean_bitString[counter] = false;
						}
					}
				} else if (current_firstBitIndex >= firstBitIndex) {
					// Case 2 : An update has occured.
					bin_firstBitIndex = Integer.toBinaryString(firstBitIndex)
							.toCharArray();
					for (int counter = 0; counter < FIRST_ONE_BIT_LENGTH; counter++) {
						if (bin_firstBitIndex[counter] == '1') {
							boolean_bitString[counter] = true;
						} else {
							boolean_bitString[counter] = false;
						}
					}
				}

			}
		} catch (Exception e) {
			System.out
					.println("There is an error while updating bitmap information.\n"
							+ e.toString());
		}
	}

	/**
	 * Update the number of 1 s in the bitmap.
	 * **/
	private void UpdateOneBitCount(String recordValue, boolean add) {

		boolean[] boolean_bitString = data_dictionary.get(recordValue);
		char[] bin_onebitcount = new char[ONE_BITCOUNT_LENGTH];
		for (int counter = FIRST_ONE_BIT_LENGTH; counter < FIRST_ONE_BIT_LENGTH
				+ ONE_BITCOUNT_LENGTH; counter++) {
			if (boolean_bitString[counter] == true) {
				bin_onebitcount[counter - FIRST_ONE_BIT_LENGTH] = '1';
			} else {
				bin_onebitcount[counter - FIRST_ONE_BIT_LENGTH] = '0';
			}
		}

		int one_bit_count = Integer
				.parseInt(String.valueOf(bin_onebitcount), 2);

		if (add == true) {
			one_bit_count++;
		} else {
			one_bit_count--;
		}
		bin_onebitcount = Integer.toBinaryString(one_bit_count).toCharArray();
		for (int counter = FIRST_ONE_BIT_LENGTH; counter < FIRST_ONE_BIT_LENGTH
				+ bin_onebitcount.length; counter++) {
			if (bin_onebitcount[counter - FIRST_ONE_BIT_LENGTH] == '1') {
				boolean_bitString[counter + ONE_BITCOUNT_LENGTH
						- bin_onebitcount.length] = true;
			} else {
				boolean_bitString[counter + ONE_BITCOUNT_LENGTH
						- bin_onebitcount.length] = false;
			}
		}
	}

	private void CreateBitMapIndex() {
		// process datacolumn and createBitMapIndex
		// TODO: Implement the sub methods needed for this method
		this.ReadDataColumn();
	}

	/**
	 * Read the data column and initialize the bit index This will run only at
	 * the initialization Calling else where from the constructor is WRONG
	 * */
	private void ReadDataColumn() {
		this.current_record_count = 0;
		this.no_of_diff_values = 0;
		for (String recordvalue : this.datacoloumn) {
			Insert(recordvalue);
		}
	}

	/**
	 * Expand the bitmap index when it is full.
	 * */
	private void ExpandBitString(String key) {
		boolean[] copy = data_dictionary.get(key);
		// if (current_record_count < copy.length + expanding_index_size) {
		// data_dictionary.put(key,
		// Arrays.copyOf(copy, copy.length + expanding_index_size));
		// }else{
		// System.out.println(((current_record_count-initial_length_of_bit_index)/expanding_index_size)+":::::::::::::::::::::::::::::::::::::::::::::::::::::::::"+copy.length+expanding_index_size*(1+(current_record_count-initial_length_of_bit_index)/expanding_index_size));
		data_dictionary.put(key, Arrays.copyOf(copy, copy.length
				+ expanding_index_size
				* (1 + (current_record_count - initial_length_of_bit_index)
						/ expanding_index_size)));
		// }
	}

	/**
	 * 
	 * Insert record doesn't need to update other bitmaps.
	 * 
	 **/
	public void Insert(String recordvalue) {

		// /hash the record value and use that as the key for the data
		// dictionary
		String hashKey = hashKey(recordvalue);

		if (!hashKey.equals(DELETE_BITMAP_INDEX_NAME)) {
			this.current_record_count++;
		}

		if (!this.data_dictionary.containsKey(hashKey)) {
			int local_length_of_bit_index = this.initial_length_of_bit_index;// To
																				// make
																				// sure
																				// that
																				// the
																				// size
																				// of
																				// the
																				// array
			// is enough to insert a new record.
			int difference = this.current_record_count
					- this.initial_length_of_bit_index;
			if (difference > 0) {
				int no_of_times_expanded = (int) Math.ceil(difference
						/ this.expanding_index_size);
				local_length_of_bit_index = this.initial_length_of_bit_index
						+ no_of_times_expanded * this.expanding_index_size;
			}
			boolean[] bitstring = new boolean[INSTRUCTION_BIT_LENGTH
					+ local_length_of_bit_index];
			this.data_dictionary.put(hashKey, bitstring);
			this.UpdateFirstBit(this.current_record_count - 1, hashKey);
			// No need to minus as there may be next insert
			if (!hashKey.equals(DELETE_BITMAP_INDEX_NAME)) {
				this.no_of_diff_values++;
			}
		}

		if (this.current_record_count + INSTRUCTION_BIT_LENGTH >= this.data_dictionary
				.get(hashKey).length) {

			this.ExpandBitString(hashKey);
		}
		// System.out
		// .println(hashKey
		// + ":"
		// + (data_dictionary.get(hashKey).length - INSTRUCTION_BIT_LENGTH));
		this.data_dictionary.get(hashKey)[INSTRUCTION_BIT_LENGTH
				+ current_record_count - 1] = true;

		this.UpdateOneBitCount(hashKey, true);

	}

	private String hashKey(String recordvalue) {
		// TODO Auto-generated method stub
		int hashcode = recordvalue.hashCode();
		hashcode = Math.abs(hashcode);
		hashcode = hashcode % BIN_COUNT;
		// System.out.println(recordvalue+" : "+hashcode);
		return "HK" + hashcode;
		// return recordvalue;
	}

	/**
	 * An extra bitmap will be kept in the dictionary for deleted rows. The
	 * record will not be deleted from the bitmap. As many 0 are there this
	 * bitmap is highly compressible. Assumption ==> id = rowNumber
	 **/
	public void Delete(int rowNumber, String recordvalue) {// The recordvalue
															// should be in the
															// index.
		String hashKey = hashKey(recordvalue);
		this.data_dictionary.get(hashKey)[INSTRUCTION_BIT_LENGTH + rowNumber
				- 1] = false;// set the bitmap to 0
		this.Insert(DELETE_BITMAP_INDEX_NAME); // Update the dele bit map.
		// this.current_record_count--;
	}

	/**
	 * Update needs to iterate through dictionary. Yet it is constant time.
	 * **/
	public void Update(int rowNumber, String recordvalue) {
		String hashKey = hashKey(recordvalue);
		for (String key : this.data_dictionary.keySet()) {
			if (this.data_dictionary.get(key)[INSTRUCTION_BIT_LENGTH
					+ rowNumber - 1] == true) {
				this.data_dictionary.get(key)[INSTRUCTION_BIT_LENGTH
						+ rowNumber - 1] = false;
				this.UpdateOneBitCount(key, false);
				this.UpdateFirstBit(-(rowNumber - 1), key);
				break;
			}
		}
		if (!this.data_dictionary.containsKey(hashKey)) {
			int local_length_of_bit_index = this.initial_length_of_bit_index;// To
																				// make
																				// sure
																				// that
																				// the
																				// size
																				// of
																				// the
																				// array
			// is enough to insert a new record.
			int difference = this.current_record_count
					- this.initial_length_of_bit_index;
			if (difference > 0) {
				int no_of_times_expanded = (int) Math.ceil(difference
						/ this.expanding_index_size);
				local_length_of_bit_index = this.initial_length_of_bit_index
						+ no_of_times_expanded * this.expanding_index_size;
			}
			boolean[] bitstring = new boolean[INSTRUCTION_BIT_LENGTH
					+ local_length_of_bit_index];
			this.data_dictionary.put(hashKey, bitstring);
			this.UpdateFirstBit(rowNumber - 1, hashKey);
			this.no_of_diff_values++;
		} else {
			// update the recordvalue
			this.UpdateFirstBit(-(rowNumber - 1), hashKey);
		}
		// Expansion is not needed if the key is already in the dictionary
		// It should have been updated before
		this.data_dictionary.get(hashKey)[INSTRUCTION_BIT_LENGTH + rowNumber
				- 1] = true;
		this.UpdateOneBitCount(hashKey, true);
	}

	public void Print() {
		// TODO: Implement the sub methods needed for this method
		System.out.println("Record count = " + current_record_count);
		for (String key : data_dictionary.keySet()) {
			System.out.println(key + "\n\n\nFIRSTONEBIT - ");
			int counter = 0;
			for (boolean b : data_dictionary.get(key)) {
				if (b == true) {
					System.out.print('1');
				} else {
					System.out.print('0');
				}
				counter++;
				if (counter == FIRST_ONE_BIT_LENGTH) {
					System.out.println("\nONEBITCOUNT - ");
				}
				if (counter == FIRST_ONE_BIT_LENGTH + ONE_BITCOUNT_LENGTH) {
					System.out.println("\nREST - ");
				}
				if (counter == 64) {
					System.out.println("\nBitmap - ");
				}
			}
			System.out.println("\n\n\n---------------------------------------");
		}
	}

	public List<Integer> getIdArray(String recordValue) {
		// long startTime = System.currentTimeMillis();
		String hashKey = hashKey(recordValue);
		List<Integer> ans = new ArrayList<Integer>();
		boolean[] boolArray = data_dictionary.get(hashKey);
		if (boolArray != null) {
			for (int i = INSTRUCTION_BIT_LENGTH; i < boolArray.length; i++) {
				if (boolArray[i]) {
					// System.out.print((i+1)+",");
					ans.add(i + 1 - INSTRUCTION_BIT_LENGTH);
				}
			}
		}
		// long endTime=System.currentTimeMillis();
		// System.out.println("id array time "+(endTime-startTime));
		return ans;
	}
}
