
package chordvalues;

import datastructures.Key;
import de.uniba.wiai.lspi.chord.service.ServiceException;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
/* This class implements data structure to hold files in Ring as Value 
it implements serializable to serialize data over network 
*/

public final class FileasValue implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2144157610883545353L;
	/**
	 * 
	 */
	public String value;
        public byte[] filebytes;

	
	public FileasValue(String filename,String appid,int chunkno) throws FileNotFoundException, IOException, ServiceException {
                File file=new File(filename);
                this.value=filename;
               filebytes=Files.readAllBytes(file.toPath());
              
               chordtest.Chordtest.chord.insert(new Key(appid+"input"+chunkno), this);
                
               // filebytes=Files.readAllBytes(filename);
                
                
                
	}

	public String toString() {
		return this.value;
	}

	public boolean equals(Object o) {
		if (o instanceof FileasValue) {
			return (this.value.equals(((FileasValue) o).value));
		}
		return false;
	}

	public int hashCode() {
		return this.value.hashCode();
	}

}
