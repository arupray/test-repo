package com.ssi.data.pipeline.validation;

import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.commons.validator.routines.*;
/**
 * 
 * @author Arup Ray
 * Last updated:
 */
public final class IsDate extends UDF{
	/**
	 * 
	 * @param s of type Hadoop IO Text, pattern is a Java date pattern like yyyy-MM-dd HH:mm:ss
	 * @return boolean - true if s is a Date, false otherwise
	 */
	public boolean evaluate(Text s, Text pattern) {
		if (s == null)
			return false;
		new DateValidator();
		DateValidator ssiDateValidator = DateValidator.getInstance();

		Date dateValue = ssiDateValidator.validate(s.toString(), pattern.toString());
		if (dateValue == null) 
			return false;
		else return true;	
	  }
	public boolean evaluate(Text s) {
		if (s == null)
			return false;
		new DateValidator();
		DateValidator ssiDateValidator = DateValidator.getInstance();

		Date dateValue = ssiDateValidator.validate(s.toString(), "yyyy-MM-dd HH:mm:ss");
		if (dateValue == null) 
			dateValue = ssiDateValidator.validate(s.toString(), "yyyy/MM/dd HH:mm:ss");
		if (dateValue == null)
			return false;
		else return true;	
	  }
}
