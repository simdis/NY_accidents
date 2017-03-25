package hadoop.ny_accidents.util;

import hadoop.ny_accidents.map_attributes.NYPDAttributes;
import hadoop.ny_accidents.types.WeekYear;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

/**
 * After setting the path. This class can count the number of weeks of the dataset at the given path.
 * Created by fusiled on 07/03/17.
 */
public class NumberOfWeekProducer {


    private static int n_weeks = -1;
    private static String path = null;
    private static int N_WEEKS_NOT_COMPUTED = -1;

    public NumberOfWeekProducer() {
        if (n_weeks == N_WEEKS_NOT_COMPUTED) {
            try {
                n_weeks = numberOfWeeksProducer();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * set path and compute n_week.
     * @param path String path to the dataset
     */
    public NumberOfWeekProducer(String path) {
        this.path = path;
        if (n_weeks == N_WEEKS_NOT_COMPUTED) {
            try {
                n_weeks = numberOfWeeksProducer();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @return the number of weeks of the dataset, or -1 if it's not possible
     * @throws IOException it is not possible to open the file at the given path
     */
    private static int numberOfWeeksProducer() throws IOException {
        if (path == null) {
            return N_WEEKS_NOT_COMPUTED;
        }
        FileReader inputFileReader = new FileReader(path);
        BufferedReader inputBufRead = new BufferedReader(inputFileReader);
        Set<WeekYear> wySet = new HashSet<>();
        String buffer = inputBufRead.readLine();
        while (buffer != null) {
            String[] tokens = new StringParser().split(buffer, ',', '"');
            WeekYear weekYear = null;
            try {
                weekYear = new WeekYearBuilder().build(tokens[NYPDAttributes.DATE.get()]);
            } catch (ParseException e) {
                System.out.println("Error on parsing the date string: " + tokens[NYPDAttributes.DATE.get()]);
                System.out.println("with entry: "+buffer);
                buffer = inputBufRead.readLine();
                continue;
            }
            wySet.add(weekYear);
            buffer = inputBufRead.readLine();
        }
        inputBufRead.close();
        inputFileReader.close();
        return wySet.size();
    }

    /**
     * Getter of n_weeks
     * @return the number of weeks in the dataset
     * @throws RuntimeException If the number of weeks cannot be computed
     */
    public int getNumberOfWeeks() throws RuntimeException {
        if (n_weeks == N_WEEKS_NOT_COMPUTED) {
            throw new RuntimeException();
        }
        return n_weeks;
    }

}
