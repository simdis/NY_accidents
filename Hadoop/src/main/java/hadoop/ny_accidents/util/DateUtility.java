package hadoop.ny_accidents.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * DateUtility is a class for simple date operations.
 * Given a stored data format, it is able to parse the a string in order to build a Date object
 * and to provide advanced parsing to the string to achieve the pair week-year.
 * @author Simone Disabato
 *
 */
public class DateUtility {
    private DateFormat dateFormat;

    /**
     * The constructor builds the empty parameters.
     * The date format can be changed, but by default is created as 
     * the AngloSaxon one ("MM/dd/yyyy"). The string to be parsed can be 
     * set in any moment and it will be parsed immediately and stored.
     */
    public DateUtility () {
        dateFormat = new SimpleDateFormat("MM/dd/yyyy");
    }
    
    public void setDateFormat(DateFormat dateFormat) {
        this.dateFormat = dateFormat;
    }
    
    public DateFormat getDateFormat() {
        return dateFormat;
    }
    
    /**
     *  Parse the date string to compute the correct pair (week,year) taking into account some
     *  specific cases.
     *  In fact, it may happens that some specific dates (e.g., the 12/31/2012) are
     *  inserted into the first week of the subsequent year in order to have exactly 52
     *  weeks per year. Vice versa, the first days of a year may be in the last week of
     *  the previous year. 
     * @param dateString the String representing the date to be parsed.
     * @return an object WeekYear containing the getters to access both the integer week and the integer year.
     * @throws ParseException when the parsing fails. See the documentation of the DateFormat classes for further info.
     */
    public WeekYear computeWeekYear(String dateString) throws ParseException {
        Date date = parseString(dateString);
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
        return new WeekYear(year,week);
    }
    
    /**
     * Parse a String according to the stored date format.
     * @param dateString the String representing the date to be parsed.
     * @return the Date object representing the parsed string.
     * @throws ParseException when the parsing fails. See the documentation of the DateFormat classes for further info.
     */
    public Date parseString(String dateString) throws ParseException {
        return dateFormat.parse(dateString);
    }

}
