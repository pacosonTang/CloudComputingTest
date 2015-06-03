package com.variance.abalone;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Sample class
 * @author hadoop
 *
 */
//structure: sex,length,diameter,height,whole-weight,shucked-weight,viscera-weight,shell-weight,rings
public class SecondClass implements Writable {  
    private String value;  
  
    public String getValue() {  
        return value;  
    }  
  
    public void setValue(String value) {  
        this.value = value;  
    }  
  
    public SecondClass() {  
        super();  
        // TODO Auto-generated constructor stub  
    }  
  
    public SecondClass(String value) {  
        super();  
        this.value = value;  
    }  
  
    @Override  
    public void write(DataOutput out) throws IOException {  
        out.writeUTF(this.value);  
    }  
  
    @Override  
    public void readFields(DataInput in) throws IOException {  
        this.value = in.readUTF();  
    }  
  
    @Override  
    public String toString() {  
        return value;  
    }  
}  