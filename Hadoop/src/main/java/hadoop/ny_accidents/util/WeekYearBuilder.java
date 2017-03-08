package hadoop.ny_accidents.util;

import hadoop.ny_accidents.types.WeekYear;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Build WeekYear. It uses a singleton pattern. It uses the Anglo-saxon format to build a WeekYear instance after
 * calling the build method.
 * Created by fusiled on 28/02/17.
 * Original version by Simone Disabato
 */
public class WeekYearBuilder {

    private static WeekYearBuilderInstance wybi = null;

    public WeekYearBuilder() {
        if (this.wybi == null) {
            this.wybi = new WeekYearBuilderInstance();
        }
    }

    /**
     * @param dateString the string of the string containing the date. It must in the ango-saxon format.
     * @return A WeekYear instance related to the passed dateString
     * @throws ParseException If dateString does not conform the the angloSaxon date this exception rises.
     */
    public WeekYear build(String dateString) throws ParseException {
        return this.wybi.build(dateString);
    }


    private class WeekYearBuilderInstance {

        private final DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

        public WeekYear build(String dateString) throws ParseException {
            Date date = dateFormat.parse(dateString);
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
            return new WeekYear(year, week);
        }

    }
}
