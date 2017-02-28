package hadoop.ny_accidents.util;

import java.util.LinkedList;
import java.util.List;

/**
 * A simple string parser that allows the user to parse a string splitting them on a specific character.
 * The advanced functionality is the fact that the split character between two quoting characters are simply ignored.
 * @author Simone Disabato
 *
 */
public class StringParser {
    boolean isQuoted;
    List<String> tokens;
    
    public StringParser() {
        isQuoted = false;
        tokens = new LinkedList<String>();
    }
    
    /**
     * Split the string on a given character ignoring the quoted parts of the String. It raises an
     * exception if the number of quoting characters is not even or if the parameters are not valid.
     * @param string The String to be parsed.
     * @param splitCharacter The character on which split.
     * @param quoteCharacter The quoting character.
     * @return
     */
    public String[] split(String string, char splitCharacter, char quoteCharacter) {
        // Check iff the parameters are valid.
        if (splitCharacter == quoteCharacter) {
            throw new RuntimeException("The split and the quote characters must be different");
        }
        // Initially we have to clear the list.
        tokens.clear();
        String token = "";
        for (int i=0;i<string.length();i++) {
            char c = string.charAt(i);
            if (c == quoteCharacter) {
                isQuoted = !isQuoted;
            }
            if (!isQuoted && c == splitCharacter) {
                tokens.add(token);
                token = "";
            } else {
                token += c;
            }
        }
        // Add the last token to the list
        tokens.add(token);
        // Check if the number of quoting characters is correct
        if (isQuoted) {
            throw new RuntimeException("The number of quoting characters is odd.");
        }
        return tokens.toArray(new String[tokens.size()]);
    }

}
