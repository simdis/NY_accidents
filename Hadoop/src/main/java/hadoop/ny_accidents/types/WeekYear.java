package hadoop.ny_accidents.types;

/**
 * A simple class representing a pair week-year, with the getters.
 * No functionality is implemented, including functions to compare the objects.
 *
 * @author Simone Disabato
 */
public class WeekYear implements Comparable<WeekYear> {
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

    @Override
    public int compareTo(WeekYear obj) {
        if (obj == null) {
            throw new NullPointerException();
        }
        if (this.equals(obj)) {
            return 0;
        }

        if (this.year < obj.year ||
                (this.year == obj.year && this.week < obj.week)) {
            return -1;
        }
        return +1;
    }

    @Override
    public int hashCode() {
        final int prime = 43;
        int result = 1;
        result = prime * result + this.year;
        result = prime * result + this.week;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof WeekYear)) {
            return false;
        }
        WeekYear other = (WeekYear) obj;
        if (this.year != other.year) {
            return false;
        }
        if (this.week != other.week) {
            return false;
        }
        return true;
    }
}
