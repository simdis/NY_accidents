import hadoop.ny_accidents.types.WeekYear;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by fusiled on 15/03/17.
 */
public class Test {

    public static void main(String [] args) throws ParseException {


        Date date = new SimpleDateFormat("MM/dd/yyyy").parse("07/02/2012");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        int month = calendar.get(Calendar.MONTH);
        int week = calendar.get(Calendar.WEEK_OF_YEAR);
        int year = calendar.get(Calendar.YEAR);

        if (month == Calendar.DECEMBER && week == 1) {
            year++;
        } else if (month == Calendar.JANUARY && week == 52) {
            year--;
        }
        WeekYear wy = new WeekYear(year,week);
        System.out.println(wy.toString());

    }
}
