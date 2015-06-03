package com.variance.abalone;

/**
* 对数据结构 10@diameter	0.06229671 进行foramt
* @author hadoop
*结果处理后的数据到顺序是（从左到右）
*1@whole_weight	1.121705E-6
*1@viscera_weight	2.8042626E-7
*1@shucked_weight	5.608525E-7
*1@shell_weight	8.4127873E-7 
*1@rings	5.608525E-4
*1@male	0.0
*1@length	4.2063937E-5
*1@infant	5.608525E-4
*1@height	5.608525E-6
*1@female	0.0
*1@diameter	3.0846888E-5
*/

public class AbaloneMeans {
	
	private String whole_weight;
	private String viscera_weight;
	private String shucked_weight;
	private String shell_weight;
	private String rings;
	private String male;
	private String length;
	private String infant;
	private String height;
	private String female;
	private String diameter;
	
	/**
	 * 构造方法
	 */
	public AbaloneMeans(String whole_weight, String viscera_weight,
			String shucked_weight, String shell_weight, String rings,
			String male, String length, String infant, String height,
			String female, String diameter) {
		this.whole_weight = whole_weight;
		this.viscera_weight = viscera_weight;
		this.shucked_weight = shucked_weight;
		this.shell_weight = shell_weight;
		this.rings = rings;
		this.male = male;
		this.length = length;
		this.infant = infant;
		this.height = height;
		this.female = female;
		this.diameter = diameter;
	}
	
	/*
	 * get和set方法
	 */
	public String getWhole_weight() {
		return whole_weight;
	}
	public void setWhole_weight(String whole_weight) {
		this.whole_weight = whole_weight;
	}
	public String getViscera_weight() {
		return viscera_weight;
	}
	public void setViscera_weight(String viscera_weight) {
		this.viscera_weight = viscera_weight;
	}
	public String getShucked_weight() {
		return shucked_weight;
	}
	public void setShucked_weight(String shucked_weight) {
		this.shucked_weight = shucked_weight;
	}
	public String getShell_weight() {
		return shell_weight;
	}
	public void setShell_weight(String shell_weight) {
		this.shell_weight = shell_weight;
	}
	public String getRings() {
		return rings;
	}
	public void setRings(String rings) {
		this.rings = rings;
	}
	public String getMale() {
		return male;
	}
	public void setMale(String male) {
		this.male = male;
	}
	public String getLength() {
		return length;
	}
	public void setLength(String length) {
		this.length = length;
	}
	public String getInfant() {
		return infant;
	}
	public void setInfant(String infant) {
		this.infant = infant;
	}
	public String getHeight() {
		return height;
	}
	public void setHeight(String height) {
		this.height = height;
	}
	public String getFemale() {
		return female;
	}
	public void setFemale(String female) {
		this.female = female;
	}
	public String getDiameter() {
		return diameter;
	}
	public void setDiameter(String diameter) {
		this.diameter = diameter;
	}
}
