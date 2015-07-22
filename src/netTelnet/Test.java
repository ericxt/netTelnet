package netTelnet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Calendar;
import java.util.Date;
import java.util.Formatter.BigDecimalLayoutForm;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		BigDecimal a = new BigDecimal(0);
		System.out.println(a.equals(BigDecimal.ZERO));
		BigDecimal b = new BigDecimal(48356.98);
		BigDecimal subtract = a.subtract(b);
		System.out.println(subtract.setScale(2, BigDecimal.ROUND_HALF_DOWN));
	}

}
