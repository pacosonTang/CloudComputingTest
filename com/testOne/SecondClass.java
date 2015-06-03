package com.testOne;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SecondClass implements Writable {  
    private String username;  
    private int classNo;  
  
    public SecondClass() {  
        super();  
    }  
  
    public SecondClass(String username, int classNo) {  
        super();  
        this.username = username;  
        this.classNo = classNo;  
    }  
  
    public String getUsername() {  
        return username;  
    }  
  
    public void setUsername(String username) {  
        this.username = username;  
    }  
  
    public int getClassNo() {  
        return classNo;  
    }  
  
    public void setClassNo(int classNo) {  
        this.classNo = classNo;  
    }  
  
    @Override  
    public void write(DataOutput out) throws IOException {  
        out.writeUTF(username);  
        out.writeInt(classNo);  
    }  
  
    @Override  
    public void readFields(DataInput in) throws IOException {  
        this.username = in.readUTF();  
        this.classNo = in.readInt();  
    }  
  
    @Override  
    public String toString() {  
        return "SecondClasst" + username + "t" + classNo;  
    }  
  
}  