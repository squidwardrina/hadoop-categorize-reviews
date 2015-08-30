import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Hadoop's review data writable.
 */
public class ReviewDataWritable implements Writable {
	private static final String DELIM = ":";
	private Text revID = new Text();
	private IntWritable data = new IntWritable();

	public ReviewDataWritable() {
		super();
	}

	// Java getters:
	public String getRevID() {
		return revID.toString();
	}

	public Integer getData() {
		return data.get();
	}

	/**
	 * Set the data using java's variables.
	 * 
	 * @param revID
	 *            the rev id to set
	 * @param data
	 *            rev's data to set
	 */
	public void set(String revID, Integer data) {
		if (data == null) {
			data = 0; // take care of case where there's no data
		}
		this.revID.set(revID);
		this.data.set(data);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		revID.readFields(in);
		data.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChars(revID.toString());
		out.writeInt(data.get());
	}

	@Override
	public String toString() {
		return revID.toString() + DELIM + data.toString();
	}
}
