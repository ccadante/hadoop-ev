package myorg.seqconvert;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class LocalToSeq {

	private static LocalSetup setup;
	private static File root;

    private static SequenceFile.Writer OpenOutputFile(File outputFile) throws Exception {
        Path outputPath = new Path(outputFile.getAbsolutePath() + ".seq");
        return SequenceFile.createWriter(setup.getLocalFileSystem(), setup.getConf(),
                                         outputPath,
                                         Text.class, BytesWritable.class,
                                         SequenceFile.CompressionType.BLOCK);
    }
    
    private static byte[] GetFileBytes(File input) throws IOException
    {
    	int bufsize = 1024;
    	
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(input));        
        ByteArrayOutputStream out = new ByteArrayOutputStream(bufsize);        
       
        byte[] temp = new byte[bufsize];        
        int size = 0;        
        while ((size = in.read(temp)) != -1) 
        {        
            out.write(temp, 0, size);        
        }        
        in.close();        
       
        byte[] content = out.toByteArray();        
//        System.out.println("Readed bytes count:" + content.length);      
        return content;
    }       
    
	private static void DirToSeq(File dir) throws Exception
	{
		if(dir.isFile())
			return;
		SequenceFile.Writer output = null;
		output = OpenOutputFile(dir);
				
		File files[] = dir.listFiles();
		for (int i=0; i<files.length; i++)
		{
			if(files[i].isDirectory())
				continue;
			String filename = files[i].getName();
			byte[] data = GetFileBytes(files[i]);
			
            Text key = new Text(dir.getName() + ".seq/" + filename);
            BytesWritable value = new BytesWritable(data);
            output.append(key, value);
		}
		
		output.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
        if (args.length != 1) {
            exitWithHelp();
        }

        try {
        	setup = new LocalSetup();
        	root = new File(args[0]);
        	if(root.isFile())
        		System.exit(1);
        	File subroot[] = root.listFiles();
        	for(int i=0; i<subroot.length; i++)
        	{
        		DirToSeq(subroot[i]);
        		System.out.println("Finished: " + subroot[i].getName());
        	}
        } catch (Exception e) {
            e.printStackTrace();
            exitWithHelp();
        }
    }

    public static void exitWithHelp() {
        System.err.println("Usage: java myorg.seqconvert.LocalToSeq <input directory>\n");
        System.exit(1);
    }
}
