package hadoop.ny_accidents.util;

/**
 * A simple class representing a pair week-year, with the getters.
 * No functionality is implemented, including functions to compare the objects.
 * @author Simone Disabato
 *
 */
public class WeekYear {
    private int year;
    private int week;

    public WeekYear(int year, int week) {
        this.year = year;
        this.week = week;
    }

    public int getYear() {
        return year;
    }

    public int getWeek() {
        return week;
    }
}
