package myorg.seqconvert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ListSequenceFile {

    private String inputFile;
    private LocalSetup setup;

    public ListSequenceFile() throws Exception {
        setup = new LocalSetup();
    }

    /** Set the name of the input sequence file.
     *
     * @param filename   a local path string
     */
    public void setInput(String filename) {
        inputFile = filename;
    }

    /** Runs the process. Keys are printed to standard output;
     * information about the sequence file is printed to standard
     * error. */
    public void execute() throws Exception {
        Path path = new Path(inputFile);
        SequenceFile.Reader reader = 
            new SequenceFile.Reader(setup.getLocalFileSystem(), path, setup.getConf());

        try {
            System.err.println("Key type is " + reader.getKeyClassName());
            System.err.println("Value type is " + reader.getValueClassName());
            if (reader.isCompressed()) {
                System.err.println("Values are compressed.");
            }
            if (reader.isBlockCompressed()) {
                System.err.println("Records are block-compressed.");
            }
            System.err.println("Compression type is " + reader.getCompressionCodec().getClass().getName());
            System.err.println("");

            Text key = (Text)(reader.getKeyClass().newInstance());
            BytesWritable value = (BytesWritable)reader.getValueClass().newInstance();
            int num = 0;
            while (reader.next(key, value)) {
                num++;
                System.out.println(key.toString() + "; " + value.getLength() + "; " + num);
            }
        } finally {
            reader.close();
        }
    }

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
        try {
        	ListSequenceFile me = new ListSequenceFile();
        	me.setInput("seqimgsamples/32m_3.seq");
			me.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
